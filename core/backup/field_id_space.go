package backup

import (
	"fmt"
	"slices"
	"strings"

	"github.com/milvus-io/milvus/pkg/v3/common"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

// Restore rebuilds a collection with field IDs of its own: CreateCollection
// assigns user field IDs sequentially from common.StartOfUserFieldID in schema
// order, and fields added later get max+1 (rootcoord's assignment rules). That
// replay only lands on the source's field IDs — which the binlog import
// matches data on — when the source's ID space is contiguous. A field dropped
// after creation leaves a hole: the replay shifts every later field to a lower
// ID and the import misplaces or silently loses data, or the guessed $meta
// split strands the vector field on the add-field side, which neither
// CreateCollection (no vector) nor AddField (vector not allowed) accepts. The
// checks here refuse to back up a collection whose ID space is provably not
// reproducible, rather than write a backup that cannot be restored.
//
// Late struct array fields are not modeled: restore hoists every struct field
// into CreateCollection, which can misalign IDs even in a contiguous space.
// That shape is rare enough to stay outside this check.

// idHole is a run of missing field IDs, bounds inclusive.
type idHole struct{ from, to int64 }

func (h idHole) String() string {
	if h.from == h.to {
		return fmt.Sprintf("%d", h.from)
	}
	return fmt.Sprintf("%d-%d", h.from, h.to)
}

// checkFieldIDSpaceFromMeta checks a collected backup schema. When the etcd
// dynamic field task has run, the real $meta field is part of the fields and
// the whole ID space is known.
func checkFieldIDSpaceFromMeta(coll string, schema *backuppb.CollectionSchema) error {
	ids := make([]int64, 0, len(schema.GetFields()))
	for _, field := range schema.GetFields() {
		if common.IsSystemField(field.GetFieldID()) {
			continue
		}
		ids = append(ids, field.GetFieldID())
	}
	for _, structField := range schema.GetStructArrayFields() {
		ids = append(ids, structField.GetFieldID())
		for _, field := range structField.GetFields() {
			ids = append(ids, field.GetFieldID())
		}
	}

	metaVisible := false
	for _, field := range schema.GetFields() {
		if field.GetIsDynamic() {
			metaVisible = true
		}
	}

	return checkFieldIDSpaceIDs(coll, schema.GetEnableDynamicField(), metaVisible, ids)
}

// checkFieldIDSpaceIDs reports the holes in a sorted-able ID list and decides
// whether restore can reproduce the IDs around them.
//
// With the full space known (dynamic schema off, or the real $meta field
// present) any hole is fatal. With dynamic schema on but the $meta slot
// invisible, exactly one narrow non-leading hole may be the $meta slot — the
// guess restore falls back to — so that shape stays allowed; every wider or
// additional hole proves dropped fields and is fatal.
func checkFieldIDSpaceIDs(coll string, enableDynamic, metaVisible bool, ids []int64) error {
	slices.Sort(ids)
	holes := fieldIDHoles(ids)
	if len(holes) == 0 {
		return nil
	}

	decidable := !enableDynamic || metaVisible
	narrowMetaSlot := len(holes) == 1 &&
		holes[0].from == holes[0].to &&
		holes[0].from != common.StartOfUserFieldID
	if decidable || !narrowMetaSlot {
		return fmt.Errorf("collection %q: field id space is not contiguous (missing %s, present %v): "+
			"restore re-creates the collection and replays field ids from %d, and binlog import matches data by field id, "+
			"so the holes — typically fields dropped after creation — would shift every later id and misplace or lose data; "+
			"refusing to write a backup that cannot be restored",
			coll, joinHoles(holes), ids, common.StartOfUserFieldID)
	}

	return nil
}

// fieldIDHoles lists the runs of missing IDs between common.StartOfUserFieldID
// and the last given ID. The caller must sort ids first; a first ID above the
// floor shows up as a leading hole.
func fieldIDHoles(ids []int64) []idHole {
	var holes []idHole
	next := int64(common.StartOfUserFieldID)
	for _, id := range ids {
		if id > next {
			holes = append(holes, idHole{from: next, to: id - 1})
		}
		next = id + 1
	}
	return holes
}

func joinHoles(holes []idHole) string {
	parts := make([]string, 0, len(holes))
	for _, hole := range holes {
		parts = append(parts, hole.String())
	}
	return strings.Join(parts, ", ")
}
