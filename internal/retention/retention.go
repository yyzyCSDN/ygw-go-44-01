// Package retention removes old segments while keeping consumer offsets valid.
package retention

import (
	"time"

	"eventbus/internal/model"
	"eventbus/internal/offset"
	"eventbus/internal/partition"
)

// Cleaner deletes segments older than the retention window.
type Cleaner struct {
	store   *partition.Store
	offsets *offset.Manager
	now     func() time.Time
}

// New creates a retention cleaner.
func New(s *partition.Store, o *offset.Manager) *Cleaner {
	return &Cleaner{store: s, offsets: o, now: time.Now}
}

// Clean removes expired segments, never dropping segments that still contain
// messages a consumer has not committed past.
//
// A segment is dropped only when two conditions hold:
//   - it is age-expired (its newest message is older than cutoff), and
//   - every consumer has committed past the end of the segment, i.e. the
//     committed offset is at or beyond the segment's end offset.
//
// The second guard is what keeps consumer offsets valid: deleting a segment a
// consumer is still reading would leave its committed offset pointing at a
// segment that no longer exists, losing the unread data.
func (c *Cleaner) Clean(p *model.Partition, seg string, cutoff time.Time) int {
	msgs := c.store.Read(p.ID, seg, 0)
	if len(msgs) == 0 {
		return 0
	}
	latest := msgs[len(msgs)-1]
	if latest.Timestamp.After(cutoff) {
		return 0
	}
	// Keep the segment while a consumer has not yet read past it. The committed
	// offset and segment end share one partition-wide offset space, so
	// committed < end means unread messages remain in this segment. Note: the
	// offset manager tracks a single position per partition; if multiple
	// consumer groups are introduced, this must become the min committed offset
	// across groups.
	if committed := c.offsets.Committed(p.ID); committed < c.store.SegmentEnd(p.ID, seg) {
		return 0
	}
	c.store.DropSegment(p.ID, seg)
	return len(msgs)
}
