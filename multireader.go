package multifile

import (
	//"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
)

//var ErrOOC = errors.New("Out of capacity")

type ReaderPooler interface {
	NewConstrainedReader() (c ConstrainedReader, err error)
	Close() error
}

type ConstrainedReader struct {
	r         io.Reader
	capacity  uint64
	bytesread uint64
}

type MultiReader struct {
	index int
	crs   []ConstrainedReader
	pool  ReaderPooler
}

func NewMultiReader(p ReaderPooler) *MultiReader {
	var m MultiReader
	m.index = 0
	m.pool = p
	return &m
}

func (m *MultiReader) AddConstrainedReader(r io.Reader, capacity uint64) {
	cr := ConstrainedReader{r, capacity, 0}
	m.crs = append(m.crs, cr)
}

func (m *MultiReader) AddConstrainedReaderFromPool() error {
	if m.pool == nil {
		return ErrPoolExhausted
	}
	c, err := m.pool.NewConstrainedReader()
	if err != nil {
		return err
	}
	m.crs = append(m.crs, c)
	return nil
}

func (m *MultiReader) Read(p []byte) (n int, err error) {
	//fmt.Println("MultiReader: requesting to read", len(p), "bytes from #", m.index)

	if len(m.crs) == 0 {
		err = m.AddConstrainedReaderFromPool()
		if err != nil {
			return 0, err
		}
	}
	if m.index < 0 {
		return 0, ErrOOC
	}
	if m.index >= len(m.crs) {
		err = m.AddConstrainedReaderFromPool()
		if err != nil {
			return 0, err
		}
		if m.index >= len(m.crs) {
			return 0, ErrPoolExhausted
		}
	}

	bytestoread := m.crs[m.index].capacity - m.crs[m.index].bytesread
	//fmt.Println("MultiReader: bytestoread=", bytestoread)
	if uint64(len(p)) < bytestoread {
		n, err = m.crs[m.index].r.Read(p)
		m.crs[m.index].bytesread += uint64(n)
		//fmt.Println("MultiReader: case 1, read", n, "bytes. Total: ", m.crs[m.index].bytesread)
		return n, err
	} else if uint64(len(p)) == bytestoread {
		n, err = m.crs[m.index].r.Read(p)
		m.crs[m.index].bytesread += uint64(n)
		//fmt.Println("MultiReader: case 2, read", n, "bytes. Total: ", m.crs[m.index].bytesread)
		if m.crs[m.index].bytesread == m.crs[m.index].capacity {
			//fmt.Println("Multiread: advancing")
			m.index++
		}
		return n, err
	} else {
		// len(p) > bytestoread
		n, err = m.crs[m.index].r.Read(p[0:bytestoread])
		m.crs[m.index].bytesread += uint64(n)
		//fmt.Println("MultiReader: case 3, read", n, "of", bytestoread, "bytes. Total: ", m.crs[m.index].bytesread)
		if m.crs[m.index].bytesread == m.crs[m.index].capacity {
			//fmt.Println("Multiread: advancing")
			m.index++
		}
		if err != nil {
			return n, err
		}
		// now read the rest
		//fmt.Println("Multiread: there are still", len(p)-n, "bytes left to be read")
		nn, ee := m.Read(p[n:])
		return n + nn, ee
	}
	return // will never happen
}

func (m *MultiReader) Close() error {
	return m.pool.Close()
}

type FileReaderPool struct {
	Path           string
	NameComponent  string
	Extension      string
	ShortExtension string
	counter        int
	PoolSize       int // needed for determining the number of digits
	FileCapacity   uint64
	digits         int
	activefile     *os.File
}

func NewFileReaderPool(path string, namecomponent string, extension string, shortextension string, poolsize int) (*FileReaderPool, error) {
	if poolsize == 0 || namecomponent == "" || extension == "" {
		return nil, ErrCreatePool
	}
	return &FileReaderPool{path, namecomponent, extension, shortextension, 0, poolsize, 0, int(math.Floor(math.Log10(float64(poolsize-1)))) + 1, nil}, nil
}

func (f *FileReaderPool) GetFileName(index int) (string, error) {
	if index < 0 || index > f.PoolSize {
		return "", ErrPoolExhausted
	}
	var name string
	if index == 0 {
		name = fmt.Sprintf("%s.%s", f.NameComponent, f.Extension)
	} else {
		format := fmt.Sprintf("%%s.%%s%%0%dd", f.digits)
		name = fmt.Sprintf(format, f.NameComponent, f.ShortExtension, index)
	}
	fpath := filepath.Join(f.Path, name)
	//fmt.Println("MultiReader: GetFileName:", index, fpath)
	return fpath, nil
}

func (f *FileReaderPool) NewConstrainedReader() (c ConstrainedReader, err error) {
	f.counter++
	if f.counter > f.PoolSize {
		f.counter--
		err = ErrPoolExhausted
		return
	}
	fpath, err := f.GetFileName(f.counter - 1)
	if err != nil {
		f.counter--
		return
	}
	if f.activefile != nil {
		f.activefile.Close()
	}
	f.activefile, err = os.Open(fpath)
	if err != nil {
		f.activefile = nil
		f.counter--
		return
	}
	fi, err := f.activefile.Stat()
	if err != nil {
		return
	}
	f.FileCapacity = uint64(fi.Size())
	c.r = f.activefile
	c.capacity = f.FileCapacity
	c.bytesread = 0
	return
}

func (f *FileReaderPool) Close() error {
	if f.activefile != nil {
		return f.activefile.Close()
	}
	return nil
}
