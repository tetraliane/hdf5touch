package hdf5touch_test

import (
	"path/filepath"
	"testing"

	"github.com/attic-labs/testify/assert"
	"github.com/tetraliane/hdf5touch"
	"gonum.org/v1/hdf5"
)

func tempFile(t *testing.T) string {
	dir := t.TempDir()
	return filepath.Join(dir, "test.h5")
}

func TestCreateGroups(t *testing.T) {
	f, err := hdf5.OpenFile(tempFile(t), hdf5.F_ACC_CREAT+hdf5.F_ACC_RDWR)
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	defer f.Close()

	err = hdf5touch.TouchGroup(f, "a/b")

	if assert.NoError(t, err) {
		assert.True(t, f.LinkExists("a"))
		assert.True(t, f.LinkExists("a/b"))
	}
}

func TestOkWhenTheGroupExists(t *testing.T) {
	f, err := hdf5.OpenFile(tempFile(t), hdf5.F_ACC_CREAT+hdf5.F_ACC_RDWR)
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	defer f.Close()

	err = hdf5touch.TouchGroup(f, "a")
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	err = hdf5touch.TouchGroup(f, "a/b")

	if assert.NoError(t, err) {
		assert.True(t, f.LinkExists("a/b"))
	}
}
