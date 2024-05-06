package worker

import (
	"github.com/c9s/goprocinfo/linux"
	"log"
)

// Stats is a wrapper around
// goprocinfo structs that represent
// memory, disk, cpu usage and average
// system load.
type Stats struct {
	MemStats  *linux.MemInfo
	DiskStats *linux.Disk
	CpuStats  *linux.CPUStat
	LoadStats *linux.LoadAvg
}

// MemTotalKb returns total usable RAM in kilobytes.
func (s *Stats) MemTotalKb() uint64 {
	return s.MemStats.MemTotal
}

// MemAvailableKb returns an estimate of
// how much memory (in kilobytes) is available for starting
// a new application (free memory or memory that can be
// potentially freed)
func (s *Stats) MemAvailableKb() uint64 {
	return s.MemStats.MemAvailable
}

// MemUsedKb returns how much memory
// is being used in kilobytes.
func (s *Stats) MemUsedKb() uint64 {
	return s.MemStats.MemTotal - s.MemStats.MemAvailable
}

// MemUsedPercent returns how much memory
// is being used in percents.
func (s *Stats) MemUsedPercent() uint64 {
	return s.MemStats.MemAvailable / s.MemStats.MemTotal
}

// DiskTotal returns total disk space.
func (s *Stats) DiskTotal() uint64 {
	return s.DiskStats.All
}

// DiskFree returns free disk space.
func (s *Stats) DiskFree() uint64 {
	return s.DiskStats.Free
}

// DiskUsed returns used disk space.
func (s *Stats) DiskUsed() uint64 {
	return s.DiskStats.Used
}

// CpuUsage returns the percentage of used CPU resources.
// Method is based on the following formula:
//
//	idle states = sum(idle sates, io wait states)
//	non-idle states = sum(user, nice, system, itq, soft irq, steal)
//	total = sum(idle states, non-idle states)
//	result = (float(total) - float(idle)) / float(total)
func (s *Stats) CpuUsage() float64 {
	idle := s.CpuStats.Idle + s.CpuStats.IOWait
	nonIdle := s.CpuStats.User + s.CpuStats.Nice +
		s.CpuStats.System + s.CpuStats.IRQ +
		s.CpuStats.SoftIRQ + s.CpuStats.Steal
	total := idle + nonIdle
	if total == 0 {
		return 0.0
	}
	return (float64(total) - float64(idle)) / float64(total)
}

func GetMemoryStats() *linux.MemInfo {
	memstats, err := linux.ReadMemInfo("/proc/meminfo")
	if err != nil {
		log.Println("[worker.Stats] [GetMemoryStats] Error reading from /proc/meminfo")
		return &linux.MemInfo{}
	}
	return memstats
}

func GetDiskStats() *linux.Disk {
	diskstats, err := linux.ReadDisk("/")
	if err != nil {
		log.Println("[worker.Stats] [GetDiskStats] Error reading from /")
		return &linux.Disk{}
	}
	return diskstats
}

func GetCpuStats() *linux.CPUStat {
	cpustats, err := linux.ReadStat("/proc/stat")
	if err != nil {
		log.Println("[worker.Stats] [GetCpuStats] Error reading from /proc/stat")
		return &linux.CPUStat{}
	}
	return &cpustats.CPUStatAll
}

func GetLoadStats() *linux.LoadAvg {
	loadstats, err := linux.ReadLoadAvg("/proc/loadavg")
	if err != nil {
		log.Println("[worker.Stats] [GetLoadStats] Error reading from /proc/loadavg")
		return &linux.LoadAvg{}
	}
	return loadstats
}

func GetStats() *Stats {
	return &Stats{
		MemStats:  GetMemoryStats(),
		DiskStats: GetDiskStats(),
		CpuStats:  GetCpuStats(),
		LoadStats: GetLoadStats(),
	}
}
