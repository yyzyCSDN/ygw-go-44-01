// Package consumer pulls messages from assigned partitions and manages leases.
package consumer

import (
	"sync"
	"time"

	"eventbus/internal/group"
	"eventbus/internal/model"
	"eventbus/internal/offset"
	"eventbus/internal/partition"
)

// Consumer reads messages for a group member.
type Consumer struct {
	mu       sync.Mutex
	group    *group.Coordinator
	offsets  *offset.Manager
	store    *partition.Store
	leases   map[int]model.Lease
	groupID  string
	memberID string
}

// New creates a consumer for a group member.
func New(g *group.Coordinator, o *offset.Manager, s *partition.Store, groupID, memberID string) *Consumer {
	return &Consumer{group: g, offsets: o, store: s, leases: make(map[int]model.Lease), groupID: groupID, memberID: memberID}
}

// AcquireLease takes a lease over a partition.
func (c *Consumer) AcquireLease(pid int, ttl time.Duration) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	if l, ok := c.leases[pid]; ok && now.Before(l.ExpiresAt) && l.Holder != c.memberID {
		return false
	}
	c.leases[pid] = model.Lease{Partition: pid, Holder: c.memberID, ExpiresAt: now.Add(ttl)}
	return true
}

// RenewLease extends a lease held by this member.
func (c *Consumer) RenewLease(pid int, ttl time.Duration) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	l, ok := c.leases[pid]
	if !ok || l.Holder != c.memberID {
		return false
	}
	l.ExpiresAt = time.Now().Add(ttl)
	c.leases[pid] = l
	return true
}

// Pull fetches messages from a partition segment starting at the durable
// offset. Reading from the durable (rather than merely committed) offset
// guarantees that after a crash we resume at the last offset whose backing
// messages were persisted, so any not-yet-durable messages are re-fetched
// instead of being silently skipped.
func (c *Consumer) Pull(pid int, seg string) []*model.Message {
	from := c.offsets.Durable(pid)
	return c.store.Read(pid, seg, from)
}

// Commit advances the committed (in-memory intent) offset for a partition.
//
// This records only the consumer's intent to advance; it does not mark the
// offset as durable. A crash between Commit and ConfirmDurable recovers from
// the previously confirmed durable offset, so any not-yet-persisted messages
// remain fetchable and will be redelivered. Pair with ConfirmDurable once the
// backing messages have been persisted.
func (c *Consumer) Commit(pid int, next int64) {
	c.offsets.Commit(pid, next)
}

// ConfirmDurable marks the offset for a partition as durably persisted, i.e.
// safe to recover from after a crash. It must be called only after the messages
// backing the offset have been flushed to durable storage, so the durable
// offset can never run ahead of the data on disk. It is clamped to the
// committed intent and never moves backwards.
func (c *Consumer) ConfirmDurable(pid int) {
	c.offsets.ConfirmDurable(pid, c.offsets.Committed(pid))
}
