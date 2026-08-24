package retention

import (
	"testing"
	"time"

	"eventbus/internal/model"
	"eventbus/internal/offset"
	"eventbus/internal/partition"
)

// appendMsgs appends n messages to seg whose timestamps start at base and
// advance 1ms each (so the newest message has timestamp base+(n-1)ms).
func appendMsgs(t *testing.T, s *partition.Store, p *model.Partition, seg string, n int, base time.Time) {
	t.Helper()
	for i := 0; i < n; i++ {
		m := model.NewMessage("m", "k", "v", p.ID)
		m.Timestamp = base.Add(time.Duration(i) * time.Millisecond)
		s.Append(p, seg, m)
	}
}

// TestClean_DropsSegmentWhenConsumerCaughtUp verifies that an age-expired
// segment is removed once every consumer has committed past its end offset.
func TestClean_DropsSegmentWhenConsumerCaughtUp(t *testing.T) {
	s := partition.New()
	om := offset.New()
	cl := New(s, om)
	p := model.NewPartition("t", 1)
	s.Add(p)

	now := time.Now()
	// messages timestamped 2h ago → older than a 1h retention cutoff → expired
	appendMsgs(t, s, p, "seg-1", 3, now.Add(-2*time.Hour))
	end := s.SegmentEnd(p.ID, "seg-1") // offset after last message

	// consumer has read everything in the segment
	om.Commit(p.ID, end)
	om.Durable(p.ID)

	removed := cl.Clean(p, "seg-1", now.Add(-time.Hour))
	if removed != 3 {
		t.Fatalf("expected 3 removed, got %d", removed)
	}
	if segs := s.Segments(p.ID); len(segs) != 0 {
		t.Fatalf("expected segment dropped, got %v", segs)
	}
}

// TestClean_KeepsSegmentWhenConsumerLagging is the regression for the reported
// bug: retention must NOT drop a segment a consumer has not yet read past,
// even when the segment is age-expired.
func TestClean_KeepsSegmentWhenConsumerLagging(t *testing.T) {
	s := partition.New()
	om := offset.New()
	cl := New(s, om)
	p := model.NewPartition("t", 1)
	s.Add(p)

	now := time.Now()
	// messages timestamped 2h ago → age-expired under a 1h retention window
	appendMsgs(t, s, p, "seg-1", 3, now.Add(-2*time.Hour))
	end := s.SegmentEnd(p.ID, "seg-1")

	// consumer committed only the first message — two remain unread
	om.Commit(p.ID, 1)
	om.Durable(p.ID)

	if end <= 1 {
		t.Fatalf("test setup: segment end %d should exceed committed 1", end)
	}

	removed := cl.Clean(p, "seg-1", now.Add(-time.Hour))
	if removed != 0 {
		t.Fatalf("expected 0 removed (consumer lagging), got %d", removed)
	}
	if segs := s.Segments(p.ID); len(segs) != 1 {
		t.Fatalf("expected segment preserved, got %v", segs)
	}
}

// TestClean_KeepsUnexpiredSegment confirms a fresh (not-yet-expired) segment is
// kept even when the consumer is fully caught up.
func TestClean_KeepsUnexpiredSegment(t *testing.T) {
	s := partition.New()
	om := offset.New()
	cl := New(s, om)
	p := model.NewPartition("t", 1)
	s.Add(p)

	now := time.Now()
	// messages timestamped now → newer than a 1h-ago cutoff → not expired
	appendMsgs(t, s, p, "seg-1", 2, now)
	end := s.SegmentEnd(p.ID, "seg-1")
	om.Commit(p.ID, end)
	om.Durable(p.ID)

	removed := cl.Clean(p, "seg-1", now.Add(-time.Hour))
	if removed != 0 {
		t.Fatalf("expected 0 removed (not expired), got %d", removed)
	}
	if segs := s.Segments(p.ID); len(segs) != 1 {
		t.Fatalf("expected segment preserved, got %v", segs)
	}
}
