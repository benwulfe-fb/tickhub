package shm

import (
	"fmt"
	"os"
	"strings"
	"unsafe"

	"golang.org/x/sys/unix"
)

// Segment wraps an mmap-backed POSIX shared memory region.
type Segment struct {
	name   string
	fd     int
	size   int64
	data   []byte
	header *GlobalHeader
}

// shmPath returns the /dev/shm path for a given name
func shmPath(name string) string {
	clean := strings.TrimPrefix(name, "/")
	if !strings.HasPrefix(clean, "tickhub_") {
		clean = "tickhub_" + clean
	}
	return "/dev/shm/" + clean
}

// OpenOrCreateSegment creates and truncates a new shared memory segment of size bytes.
func OpenOrCreateSegment(name string, size int64, perm uint32) (*Segment, error) {
	path := shmPath(name)
	if perm == 0 {
		perm = 0660
	}

	// First attempt to unlink any stale segment with the same name
	_ = unix.Unlink(path)

	fd, err := unix.Open(path, unix.O_RDWR|unix.O_CREAT|unix.O_EXCL, perm)
	if err != nil {
		// Fallback without O_EXCL
		fd, err = unix.Open(path, unix.O_RDWR|unix.O_CREAT, perm)
		if err != nil {
			return nil, fmt.Errorf("open failed for %s: %w", path, err)
		}
	}

	if err := unix.Ftruncate(fd, size); err != nil {
		_ = unix.Close(fd)
		_ = unix.Unlink(path)
		return nil, fmt.Errorf("ftruncate to %d bytes failed: %w", size, err)
	}

	data, err := unix.Mmap(fd, 0, int(size), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		_ = unix.Close(fd)
		_ = unix.Unlink(path)
		return nil, fmt.Errorf("mmap failed: %w", err)
	}

	header := (*GlobalHeader)(unsafe.Pointer(&data[0]))

	return &Segment{
		name:   path,
		fd:     fd,
		size:   size,
		data:   data,
		header: header,
	}, nil
}

// AttachSegment attaches to an existing shared memory segment in read-only or read-write mode.
func AttachSegment(name string, readOnly bool) (*Segment, error) {
	path := shmPath(name)
	flags := unix.O_RDWR
	prot := unix.PROT_READ | unix.PROT_WRITE
	if readOnly {
		flags = unix.O_RDONLY
		prot = unix.PROT_READ
	}

	fd, err := unix.Open(path, flags, 0)
	if err != nil {
		return nil, fmt.Errorf("open attach failed for %s: %w", path, err)
	}

	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("fstat failed: %w", err)
	}

	size := stat.Size
	if size < int64(unsafe.Sizeof(GlobalHeader{})) {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("invalid SHM segment size %d (too small)", size)
	}

	data, err := unix.Mmap(fd, 0, int(size), prot, unix.MAP_SHARED)
	if err != nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("mmap attach failed: %w", err)
	}

	header := (*GlobalHeader)(unsafe.Pointer(&data[0]))
	if header.Magic != MagicBytes {
		_ = unix.Munmap(data)
		_ = unix.Close(fd)
		return nil, fmt.Errorf("invalid SHM magic 0x%X (expected 0x%X)", header.Magic, MagicBytes)
	}

	return &Segment{
		name:   path,
		fd:     fd,
		size:   size,
		data:   data,
		header: header,
	}, nil
}

// Header returns the typed GlobalHeader pointer.
func (s *Segment) Header() *GlobalHeader {
	return s.header
}

// Bytes returns the raw underlying mapped byte slice.
func (s *Segment) Bytes() []byte {
	return s.data
}

// Size returns the total mapped size in bytes.
func (s *Segment) Size() int64 {
	return s.size
}

// Close unmaps the memory and closes the file descriptor.
func (s *Segment) Close(unlink bool) error {
	var errs []string
	if s.data != nil {
		if err := unix.Munmap(s.data); err != nil {
			errs = append(errs, fmt.Sprintf("munmap: %v", err))
		}
		s.data = nil
	}
	if s.fd >= 0 {
		if err := unix.Close(s.fd); err != nil {
			errs = append(errs, fmt.Sprintf("close: %v", err))
		}
		s.fd = -1
	}
	if unlink && s.name != "" {
		if err := unix.Unlink(s.name); err != nil && !os.IsNotExist(err) {
			errs = append(errs, fmt.Sprintf("unlink: %v", err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("close errors: %s", strings.Join(errs, "; "))
	}
	return nil
}
