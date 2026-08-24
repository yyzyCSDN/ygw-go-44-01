package consumer

import (
	"eventbus/internal/model"
)

// Handler processes one message and decides ack/dead-letter.
type Handler func(msg *model.Message) error

// Run consumes messages and applies the handler, acking successes.
//
// Ordering: each successful message first records a commit intent, then
// persists its data to durable storage, and only then promotes the offset to
// durable. The durable offset therefore never advances past messages that are
// actually on disk; a crash leaves the durable offset pointing at the last
// confirmed message, so un-persisted messages are re-fetched and redelivered
// rather than lost.
func (c *Consumer) Run(pid int, seg string, h Handler, max int) int {
	processed := 0
	for _, m := range c.Pull(pid, seg) {
		if max > 0 && processed >= max {
			break
		}
		if h(m) == nil {
			c.Commit(pid, m.Offset+1)
			c.store.Flush(pid, seg)
			c.ConfirmDurable(pid)
			processed++
		}
	}
	return processed
}
