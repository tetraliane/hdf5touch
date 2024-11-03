package hdf5touch

import (
	"fmt"
	"path"
	"strings"

	"gonum.org/v1/hdf5"
)

func TouchGroup(f *hdf5.File, name string) error {
	pl := strings.Split(name, "/")
	for i := range len(pl) + 1 {
		n := path.Join("/", path.Join(pl[0:i]...))
		if n == "/" {
			continue
		}
		err := makeGroup(f, n)
		if err != nil {
			return err
		}
	}
	return nil
}

func makeGroup(f *hdf5.File, name string) error {
	g, err := f.CreateGroup(name)
	if err == nil {
		// The group was successfully created.
		g.Close()
		return nil
	}
	g, err = f.OpenGroup(name)
	if err == nil {
		// There is a group already.
		g.Close()
		return nil
	}
	// There is a dataset.
	return fmt.Errorf("is not a group: %v", name)
}
