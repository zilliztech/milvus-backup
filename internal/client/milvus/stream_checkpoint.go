package milvus

import (
	"fmt"
	"maps"
	"slices"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
)

// StreamCheckpoint belongs to the restore, not to a connection. It retains
// the original split, stamped messages in source-channel order.
type StreamCheckpoint struct {
	flushBounds map[string]uint64
	mu          sync.Mutex
	lastTick    uint64
	operations  map[string]bool
	pending     map[string][]*commonpb.ImmutableMessage
	confirmed   map[string]uint64
}

func NewStreamCheckpoint(bounds map[string]uint64) *StreamCheckpoint {
	return &StreamCheckpoint{
		flushBounds: bounds,
		operations:  make(map[string]bool),
		pending:     make(map[string][]*commonpb.ImmutableMessage),
		confirmed:   make(map[string]uint64),
	}
}

func (s *StreamCheckpoint) Sent(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.operations[key]
}

func (s *StreamCheckpoint) record(key string, tick uint64, messages map[string][]*commonpb.ImmutableMessage, forward bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for ch, msgs := range messages {
		bound := s.flushBounds[ch]
		for _, msg := range msgs {
			tt, err := GetTT(msg)
			if err != nil || tt == 0 || (forward && tt != bound) || (!forward && tt >= bound) {
				return fmt.Errorf("secondary: invalid replay tick %d on %s (flush boundary %d)", tt, ch, bound)
			}
		}
	}
	s.lastTick = tick
	if key != "" {
		s.operations[key] = true
	}
	for ch, msgs := range messages {
		s.pending[ch] = append(s.pending[ch], msgs...)
	}
	return nil
}

func (s *StreamCheckpoint) confirm(ch string, tick uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	msgs := s.pending[ch]
	n := 0
	for _, msg := range msgs {
		tt, err := GetTT(msg)
		if err != nil || tt > tick {
			break
		}
		s.confirmed[ch] = tt
		n++
	}
	clear(msgs[:n])
	s.pending[ch] = msgs[n:]
}

func (s *StreamCheckpoint) replay() (uint64, map[string][]*commonpb.ImmutableMessage) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pending := make(map[string][]*commonpb.ImmutableMessage, len(s.pending))
	for ch, msgs := range s.pending {
		pending[ch] = slices.Clone(msgs)
	}
	return s.lastTick, pending
}

// ValidatePosition permits a lost ACK, but rejects a rewind or a position
// the restore never sent on this source channel. CDC must remain stopped.
func (s *StreamCheckpoint) ValidatePosition(ch string, tick uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if tick == s.confirmed[ch] {
		return nil
	}
	for _, msg := range s.pending[ch] {
		if tt, err := GetTT(msg); err == nil && tt == tick {
			return nil
		}
	}
	return fmt.Errorf("secondary: unexpected checkpoint %d on source channel %s (confirmed %d)", tick, ch, s.confirmed[ch])
}

// StreamCheckpointSnapshot contains no connection or synchronization state.
// Pending protobuf bytes preserve message IDs, headers, payloads and order.
type StreamCheckpointSnapshot struct {
	FlushBounds map[string]uint64
	LastTick    uint64
	Operations  map[string]bool
	Pending     map[string][][]byte
	Confirmed   map[string]uint64
}

func (s *StreamCheckpoint) Snapshot() (*StreamCheckpointSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := &StreamCheckpointSnapshot{FlushBounds: maps.Clone(s.flushBounds), LastTick: s.lastTick, Operations: maps.Clone(s.operations), Confirmed: maps.Clone(s.confirmed), Pending: make(map[string][][]byte)}
	for ch, msgs := range s.pending {
		for _, msg := range msgs {
			b, err := proto.Marshal(msg)
			if err != nil {
				return nil, err
			}
			out.Pending[ch] = append(out.Pending[ch], b)
		}
	}
	return out, nil
}

func RestoreStreamCheckpoint(in *StreamCheckpointSnapshot) (*StreamCheckpoint, error) {
	if in == nil {
		return nil, fmt.Errorf("secondary: missing stream checkpoint")
	}
	out := NewStreamCheckpoint(maps.Clone(in.FlushBounds))
	out.lastTick = in.LastTick
	for key, value := range in.Operations {
		out.operations[key] = value
	}
	for ch, tick := range in.Confirmed {
		out.confirmed[ch] = tick
	}
	for ch, msgs := range in.Pending {
		previous := out.confirmed[ch]
		for _, data := range msgs {
			msg := new(commonpb.ImmutableMessage)
			if err := proto.Unmarshal(data, msg); err != nil {
				return nil, err
			}
			tick, err := GetTT(msg)
			if err != nil || tick == 0 || tick < previous || tick > out.flushBounds[ch] {
				return nil, fmt.Errorf("secondary: invalid saved message tick on %s", ch)
			}
			previous = tick
			out.pending[ch] = append(out.pending[ch], msg)
		}
	}
	return out, nil
}
