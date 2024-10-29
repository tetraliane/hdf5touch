package hdf5touch

import (
	"path/filepath"
	"strings"

	"gonum.org/v1/hdf5"
)

func Touch(f *hdf5.File, name string) error {
	pl := strings.Split(name, "/")
	for i := range len(pl) + 1 {
		n := filepath.Join(pl[0:i]...)
		if len(n) == 0 {
			continue
		}
		err := touch(f, n)
		if err != nil {
			return err
		}
	}
	return nil
}

func touch(f *hdf5.File, name string) error {
	if f.LinkExists(name) {
		return nil
	}

	g, err := f.CreateGroup(name)
	if err != nil {
		return err
	}
	g.Close()
	return nil
}

