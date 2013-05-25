package multifile

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"

	"cascadier/logger"
)

var ErrOOC = errors.New("Out of capacity")
var ErrCreatePool = errors.New("Could not create pool")
var ErrPoolExhausted = errors.New("Pool exhausted")

type WriterPooler interface {
	NewConstrainedWriter() (c ConstrainedWriter, err error)
	Close() error
	//Rewind(index int) (*os.File, error)
}

type ConstrainedWriter struct {
	w            io.Writer
	capacity     uint64
	byteswritten uint64
}

type MultiWriter struct {
	index int
	cws   []ConstrainedWriter
	pool  WriterPooler
}

// You can give an empty WriterPooler here, as long as you handle the the writer yourself by AddConstrainedWriter
func NewMultiWriter(p WriterPooler) *MultiWriter {
	var m MultiWriter
	m.index = 0
	m.pool = p
	return &m
}

func (m *MultiWriter) AddConstrainedWriter(w io.Writer, capacity uint64) {
	cw := ConstrainedWriter{w, capacity, 0}
	m.cws = append(m.cws, cw)
}

func (m *MultiWriter) AddConstrainedWriterFromPool() error {
	if m.pool == nil {
		return ErrPoolExhausted
	}
	c, err := m.pool.NewConstrainedWriter()
	if err != nil {
		return err
	}
	m.cws = append(m.cws, c)
	return nil
}

func (m *MultiWriter) Write(p []byte) (n int, err error) {
	//fmt.Println("MultiWriter: requesting to write", len(p), "bytes to Write #", m.index)

	if len(m.cws) == 0 {
		err = m.AddConstrainedWriterFromPool()
		if err != nil {
			return 0, err
		}
	}
	if m.index < 0 {
		return 0, ErrOOC
	}
	if m.index >= len(m.cws) {
		err = m.AddConstrainedWriterFromPool()
		if err != nil {
			return 0, err
		}
		if m.index >= len(m.cws) {
			// this should never occur
			return 0, ErrPoolExhausted
		}
	}

	bytestowrite := m.cws[m.index].capacity - m.cws[m.index].byteswritten
	//fmt.Println("MultiWriter: bytestowrite=", bytestowrite)
	if uint64(len(p)) < bytestowrite {
		n, err = m.cws[m.index].w.Write(p)
		m.cws[m.index].byteswritten += uint64(n)
		//fmt.Println("MultiWriter: case 1, wrote", n, "bytes. Total: ", m.cws[m.index].byteswritten)
		return n, err
	} else if uint64(len(p)) == bytestowrite {
		n, err = m.cws[m.index].w.Write(p)
		m.cws[m.index].byteswritten += uint64(n)
		//fmt.Println("MultiWriter: case 2, wrote", n, "bytes. Total: ", m.cws[m.index].byteswritten)
		if m.cws[m.index].byteswritten == m.cws[m.index].capacity {
			//fmt.Println("MultiWrite: advancing")
			m.index++
		}
		return n, err
	} else {
		// len(p) > bytestowrite
		n, err = m.cws[m.index].w.Write(p[0:bytestowrite])
		m.cws[m.index].byteswritten += uint64(n)
		//fmt.Println("MultiWriter: case 3, wrote", n, "of", bytestowrite, "bytes. Total: ", m.cws[m.index].byteswritten, err)
		if m.cws[m.index].byteswritten == m.cws[m.index].capacity {
			//fmt.Println("MultiWrite: advancing")
			m.index++
		}
		if err != nil {
			return n, err
		}
		// now write the rest
		//fmt.Println("MultiWrite: there are still", len(p)-n, "bytes left to be written")
		nn, ee := m.Write(p[n:])
		return n + nn, ee
	}
	return // will never happen
}

func (f *MultiWriter) Close() error {
	return f.pool.Close()
}

/*
func (f *MultiWriter) Rewind(index int) error {
	w, err := f.pool.Rewind(index)
	if err != nil {
		return err
	}
	f.index = index
	f.cws[f.index].byteswritten = 0
	f.cws[f.index].w = w
	return nil
}
*/

type FileWriterPool struct {
	Path           string
	NameComponent  string
	Extension      string
	ShortExtension string
	counter        int
	PoolSize       int
	FileCapacity   uint64
	digits         int
	activefile     *os.File
}

func NewFileWriterPool(path string, namecomponent string, extension string, shortextension string, poolsize int, capacity uint64) (*FileWriterPool, error) {
	if poolsize == 0 || capacity == 0 || namecomponent == "" || extension == "" {
		return nil, ErrCreatePool
	}
	return &FileWriterPool{path, namecomponent, extension, shortextension, 0, poolsize, capacity, int(math.Floor(math.Log10(float64(poolsize-1)))) + 1, nil}, nil
}

func (f *FileWriterPool) GetFileName(index int) (string, error) {
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
	logger.D(1, "MultiWriter: GetFileName:", index, fpath)
	return fpath, nil
}

func (f *FileWriterPool) NewConstrainedWriter() (c ConstrainedWriter, err error) {
	logger.D(1, "NewConstrainedWriter: increasing counter", f.counter)
	f.counter++
	if f.counter > f.PoolSize {
		err = ErrPoolExhausted
		f.counter--
		return
	}
	if f.counter == 1 {
		err = os.MkdirAll(f.Path, 0700)
		if err != nil {
			f.counter--
			return
		}
	}
	fpath, err := f.GetFileName(f.counter - 1)
	if err != nil {
		f.counter--
		return
	}
	if f.activefile != nil {
		f.activefile.Close()
	}
	//f.activefile, err = os.Create(fpath)
	f.activefile, err = os.OpenFile(fpath, os.O_RDWR|os.O_CREATE, 0700)
	if err != nil {
		f.activefile = nil
		f.counter--
		return
	}
	c.w = f.activefile
	c.capacity = f.FileCapacity
	c.byteswritten = 0
	return
}

func (f *FileWriterPool) Close() error {
	if f.activefile != nil {
		f.activefile.Sync()
		return f.activefile.Close()
	} else {
		return ErrPoolExhausted
	}
	return nil
}

/*
func (f *FileWriterPool) Rewind(index int) (*os.File, error) {
	if index < 0 {
		return nil, ErrPoolExhausted
	}
	if index >= f.PoolSize {
		return nil, ErrPoolExhausted
	}
	s, err := f.GetFileName(index)
	fmt.Println("Rewind: rewinding to", index, s, err)
	if err != nil {
		return nil, err
	}
	fmt.Println("Rewind: closing", f.activefile.Name())
	f.activefile.Sync()
	err = f.activefile.Close()
	if err != nil {
		return nil, err
	}
	fmt.Println("Rewind: opening", s)
	//file, err := os.Create(s)
	file, err := os.OpenFile(s, os.O_RDWR|os.O_CREATE, 0700)
	if err != nil {
		// probably it doesn't exist!
		return nil, err
	}
	nn, err := file.Seek(0, 0)
	fmt.Println("Rewind: Seek", nn, err)
	if err != nil {
		return nil, err
	}
	f.activefile = file
	f.counter = index
	return f.activefile, nil
}
*/
