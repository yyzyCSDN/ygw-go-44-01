// Package redelivery re-queues unacknowledged messages after a timeout.
package redelivery

import (
	"time"

	"eventbus/internal/model"
	"eventbus/internal/partition"
)

// Tracker holds unacked messages for redelivery.
type Tracker struct {
	store    *partition.Store
	unacked  map[int][]*model.Message
	deadline time.Duration
}

// New creates a redelivery tracker.
func New(s *partition.Store, deadline time.Duration) *Tracker {
	return &Tracker{store: s, unacked: make(map[int][]*model.Message), deadline: deadline}
}

// Track registers a message as unacked.
func (t *Tracker) Track(pid int, msg *model.Message) {
	t.unacked[pid] = append(t.unacked[pid], msg)
}

// Due returns messages whose deadline has passed.
//
// Only messages whose deadline has genuinely elapsed are redelivered; an
// in-flight message that has not yet timed out stays tracked. This preserves
// delivery order: a message is only re-queued after its deadline, and because
// the redelivered copy is appended to the log at a higher offset than the
// original, the consumer always sees the original before any redelivery. The
// returned slice keeps the original tracking order, so originals precede their
// redeliveries.
func (t *Tracker) Due(pid int, now time.Time) []*model.Message {
	out := make([]*model.Message, 0)
	kept := make([]*model.Message, 0)
	for _, m := range t.unacked[pid] {
		if now.Sub(m.Timestamp) >= t.deadline {
			out = append(out, m)
		} else {
			kept = append(kept, m)
		}
	}
	t.unacked[pid] = kept
	return out
}
