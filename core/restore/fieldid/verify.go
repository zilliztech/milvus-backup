package fieldid

import (
	"context"
	"fmt"
	"strings"

	"github.com/milvus-io/milvus/pkg/v3/common"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/client/milvus"
	"github.com/zilliztech/milvus-backup/internal/collref"
)

// Verify reads the ids the target assigned and reports every field that is not
// the id its data is stored under. The target's own DescribeCollection is the
// ground truth here: it covers create, add field and a collection created
// outside restore alike, and it proves a server-side preserve request actually
// took effect instead of trusting a capability flag.
func Verify(ctx context.Context, cli milvus.Grpc, target collref.Name, schema *backuppb.CollectionSchema) error {
	resp, err := cli.DescribeCollection(ctx, target.DBName(), target.CollName())
	if err != nil {
		return fmt.Errorf("describe target collection: %w", err)
	}

	source, err := recordedFields(schema)
	if err != nil {
		return err
	}
	targetFields, err := describedFields(resp.GetSchema())
	if err != nil {
		return err
	}

	targetIDs := make(map[string]int64, len(targetFields))
	for _, f := range targetFields {
		targetIDs[f.name] = f.id
	}

	var mismatches []string
	for _, f := range source {
		id, ok := targetIDs[f.name]
		if !ok {
			if f.name == common.MetaFieldName || f.name == common.NamespaceFieldName {
				// Public DescribeCollection may filter hidden fields out; an
				// absent id is not an id mismatch.
				continue
			}
			mismatches = append(mismatches, fmt.Sprintf("field %q is missing from the target", f.name))
			continue
		}
		if id != f.id {
			mismatches = append(mismatches, fmt.Sprintf("field %q is id %d on the target but id %d in the backup", f.name, id, f.id))
		}
	}
	if len(mismatches) != 0 {
		return fmt.Errorf("the target does not reproduce the source field ids, so importing now would read the wrong columns: %s",
			strings.Join(mismatches, "; "))
	}

	return nil
}
