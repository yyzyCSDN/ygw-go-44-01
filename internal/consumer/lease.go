package consumer

import "time"

// LeaseLoop renews a partition lease until stop.
func (c *Consumer) LeaseLoop(pid int, ttl time.Duration, stop <-chan struct{}) bool {
	ticker := time.NewTicker(ttl / 2)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return true
		case <-ticker.C:
			c.RenewLease(pid, ttl)
		}
	}
}
