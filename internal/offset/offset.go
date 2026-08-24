// Package offset manages committed consumer offsets and durability ordering.
package offset

import (
	"sync"

	"eventbus/internal/model"
)

// Manager tracks committed offsets per partition and persists them.
type Manager struct {
	mu      sync.Mutex
	commits map[int]int64
	durable map[int]int64
}

// New creates an offset manager.
func New() *Manager {
	return &Manager{commits: make(map[int]int64), durable: make(map[int]int64)}
}

// Committed returns the committed offset of a partition.
func (m *Manager) Committed(pid int) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.commits[pid]
}

// Commit records an intent to advance the committed offset to next.
//
// This advances the in-memory intent only. It deliberately does NOT touch the
// durable offset: a committed (but not yet confirmed) offset records what the
// consumer wishes to advance to, not what is safe to recover from. Until the
// messages backing `next` have actually been persisted to disk, recovering after
// a crash must not jump the offset forward, or those messages become
// unrecoverable and are never redelivered. Call ConfirmDurable once the data is
// on disk.
func (m *Manager) Commit(pid int, next int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Advance intent; never claim durability ahead of persisted data.
	if next > m.commits[pid] {
		m.commits[pid] = next
	}
}

// ConfirmDurable advances the durable offset of a partition to off.
//
// It must be called only after the messages up to and including off-1 have been
// durably persisted, so the durable offset can never run ahead of the data on
// disk. It is clamped to the committed intent (durable never exceeds commit)
// and never moves backwards, so a late confirmation for an older offset cannot
// regress recovery state.
func (m *Manager) ConfirmDurable(pid int, off int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if off > m.commits[pid] {
		off = m.commits[pid]
	}
	if off > m.durable[pid] {
		m.durable[pid] = off
	}
}

// Durable returns the confirmed-durable offset of a partition: the highest
// offset whose backing messages are known to be persisted and therefore safe
// to recover from after a crash.
func (m *Manager) Durable(pid int) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.durable[pid]
}

// Recover restores committed offsets from durable state.
func (m *Manager) Recover() map[int]int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[int]int64, len(m.durable))
	for k, v := range m.durable {
		out[k] = v
	}
	return out
}

// State returns the offset state for a partition.
func (m *Manager) State(pid int, next int64) model.OffsetState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return model.OffsetState{Partition: pid, Committed: m.commits[pid], Next: next}
}
