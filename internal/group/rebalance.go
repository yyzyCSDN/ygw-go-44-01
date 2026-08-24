package group

import "sort"

// Rebalance reassigns partitions to the current online members round-robin
// and returns a mapping of member -> partition ids. A member whose heartbeat
// is stale is treated as offline: it receives no partitions and any previous
// assignment is cleared, so delivery never targets a downed consumer. The
// roster is recomputed from live heartbeats on every call rather than cached,
// so newly joined members take effect immediately and departed ones stop
// receiving.
func (c *Coordinator) Rebalance(group string, partitions []int) map[string][]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	g := c.groups[group]
	if g == nil || len(g.Members) == 0 {
		return map[string][]int{}
	}
	cutoff := c.now().Add(-c.heartbeatTimeout)
	members := make([]string, 0, len(g.Members))
	for id, m := range g.Members {
		// Reset every member first; only online members get partitions back.
		m.Partitions = nil
		if !m.LastHeartbeat.Before(cutoff) {
			members = append(members, id)
		}
	}
	sort.Strings(members)
	assign := make(map[string][]int)
	if len(members) > 0 {
		sorted := append([]int(nil), partitions...)
		sort.Ints(sorted)
		for i, p := range sorted {
			m := members[i%len(members)]
			assign[m] = append(assign[m], p)
		}
		for id, ps := range assign {
			if m := g.Members[id]; m != nil {
				m.Partitions = ps
			}
		}
	}
	g.Version++
	return assign
}
