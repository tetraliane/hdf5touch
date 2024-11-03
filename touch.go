package hdf5touch

import (
	"fmt"
	"path"
	"strings"

	"gonum.org/v1/hdf5"
)

func Touch(f *hdf5.File, name string, t *hdf5.Datatype) error {
	if strings.HasSuffix(name, "/") {
		return fmt.Errorf("name ends with a slash")
	}

	grp := path.Dir(name)
	err := TouchGroup(f, grp)
	if err != nil {
		return err
	}

	dSpace, err := hdf5.CreateSimpleDataspace([]uint{0}, []uint{0})
	if err != nil {
		return err
	}
	defer dSpace.Close()

	dSet, err := f.CreateDataset(name, t, dSpace)
	if err == nil {
		// The dataset was successfully created
		dSet.Close()
		return nil
	}
	dSet, err = f.OpenDataset(name)
	if err == nil {
		// There was a dataset already
		dSet.Close()
		return nil
	}
	// There was a group
	return fmt.Errorf("is not a dataset: %v", path.Join(f.Name(), name))
}

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
