package redelivery

import (
	"testing"
	"time"

	"eventbus/internal/model"
)

// TestDue_OrderPreserved reproduces the ordering bug: an in-flight message
// must NOT be redelivered ahead of an earlier message whose deadline elapsed,
// and a message that has not timed out must stay tracked rather than being
// re-queued before the consumer finishes the original.
func TestDue_OrderPreserved(t *testing.T) {
	tr := New(nil, 2*time.Second)

	// m1 was delivered first and its deadline has elapsed.
	old := &model.Message{ID: "m1", Key: "k", Value: "v", Partition: 1, Timestamp: time.Now().Add(-3 * time.Second)}
	// m2 was delivered later and is still in flight (deadline NOT elapsed).
	fresh := &model.Message{ID: "m2", Key: "k", Value: "v", Partition: 1, Timestamp: time.Now()}

	tr.Track(1, old)
	tr.Track(1, fresh)

	due := tr.Due(1, time.Now())

	// Only the expired message should be redelivered...
	if len(due) != 1 {
		t.Fatalf("expected 1 due message, got %d", len(due))
	}
	if due[0].ID != "m1" {
		t.Fatalf("expected m1 due, got %s", due[0].ID)
	}

	// ...and the in-flight message must remain tracked.
	if len(tr.unacked[1]) != 1 || tr.unacked[1][0].ID != "m2" {
		t.Fatalf("in-flight message m2 must stay tracked, got %v", tr.unacked[1])
	}
}

// TestDue_OrderWhenAllExpired keeps slice order so originals precede
// redeliveries (redelivered copies always get higher offsets via Append).
func TestDue_OrderWhenAllExpired(t *testing.T) {
	tr := New(nil, time.Second)
	m1 := model.NewMessage("m1", "k", "v", 1)
	m2 := model.NewMessage("m2", "k", "v", 1)
	m3 := model.NewMessage("m3", "k", "v", 1)
	tr.Track(1, m1)
	tr.Track(1, m2)
	tr.Track(1, m3)

	due := tr.Due(1, time.Now().Add(time.Hour))
	if len(due) != 3 {
		t.Fatalf("expected 3 due messages, got %d", len(due))
	}
	got := []string{due[0].ID, due[1].ID, due[2].ID}
	want := []string{"m1", "m2", "m3"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order mismatch at %d: got %v want %v", i, got, want)
		}
	}
}
