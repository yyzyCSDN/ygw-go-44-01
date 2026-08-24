package consumer

// Checkpoint records the durable position of a consumer group member.
type Checkpoint struct {
	Group    string
	Member   string
	Partition int
	Offset   int64
}

// Snapshot builds a checkpoint for the current durable offset. The durable
// offset is the safe recovery point: every message up to it is on disk.
func (c *Consumer) Snapshot(pid int) Checkpoint {
	return Checkpoint{
		Group:     c.groupID,
		Member:    c.memberID,
		Partition: pid,
		Offset:    c.offsets.Durable(pid),
	}
}

// Restore applies a checkpoint's offset as both the committed intent and the
// confirmed durable position, since a checkpoint captures a durable state.
func (c *Consumer) Restore(cp Checkpoint) {
	c.offsets.Commit(cp.Partition, cp.Offset)
	c.offsets.ConfirmDurable(cp.Partition, cp.Offset)
}

// Progress is a lightweight view of group progress.
type Progress struct {
	Group    string
	Member   string
	Committed int64
}

// Progress returns the consumer's committed progress for a partition.
func (c *Consumer) Progress(pid int) Progress {
	return Progress{Group: c.groupID, Member: c.memberID, Committed: c.offsets.Committed(pid)}
}
