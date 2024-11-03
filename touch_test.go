package hdf5touch_test

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tetraliane/hdf5touch"
	"gonum.org/v1/hdf5"
)

func tempFile(t *testing.T) string {
	dir := t.TempDir()
	return filepath.Join(dir, "test.h5")
}

func TestTouch_CreateDataset(t *testing.T) {
	f, err := hdf5.OpenFile(tempFile(t), hdf5.F_ACC_CREAT+hdf5.F_ACC_RDWR)
	require.NoError(t, err)
	defer f.Close()

	err = hdf5touch.Touch(f, "a/b", hdf5.T_NATIVE_INT)

	require.NoError(t, err)

	g, err := f.OpenGroup("a")
	require.NoError(t, err)
	g.Close()

	d, err := f.OpenDataset("a/b")
	require.NoError(t, err)
	defer d.Close()

	dims, maxDims, err := d.Space().SimpleExtentDims()
	require.NoError(t, err)
	assert.Equal(t, dims, []uint{0})
	assert.Equal(t, maxDims, []uint{0})
}

func TestTouch_OkWhenTheDatasetExists(t *testing.T) {
	f, err := hdf5.OpenFile(tempFile(t), hdf5.F_ACC_CREAT+hdf5.F_ACC_RDWR)
	require.NoError(t, err)
	defer f.Close()

	err = hdf5touch.Touch(f, "a", hdf5.T_NATIVE_INT)
	require.NoError(t, err)

	err = hdf5touch.Touch(f, "a", hdf5.T_NATIVE_INT)
	require.NoError(t, err)
}

func TestTouch_OkWhenTheIntermediateGroupExists(t *testing.T) {
	f, err := hdf5.OpenFile(tempFile(t), hdf5.F_ACC_CREAT+hdf5.F_ACC_RDWR)
	require.NoError(t, err)
	defer f.Close()

	err = hdf5touch.TouchGroup(f, "a")
	require.NoError(t, err)

	err = hdf5touch.Touch(f, "a/b", hdf5.T_NATIVE_INT)
	require.NoError(t, err)
}

func TestTouch_FailWhenFoundGroup(t *testing.T) {
	f, err := hdf5.OpenFile(tempFile(t), hdf5.F_ACC_CREAT+hdf5.F_ACC_RDWR)
	require.NoError(t, err)
	defer f.Close()

	err = hdf5touch.TouchGroup(f, "a")
	require.NoError(t, err)

	err = hdf5touch.Touch(f, "a", hdf5.T_NATIVE_INT)
	require.Error(t, err)
	require.EqualError(t, err, "is not a dataset: /a")
}

func TestTouch_FailWhenNameEndsWithSlash(t *testing.T) {
	f, err := hdf5.OpenFile(tempFile(t), hdf5.F_ACC_CREAT+hdf5.F_ACC_RDWR)
	require.NoError(t, err)
	defer f.Close()

	err = hdf5touch.Touch(f, "a/", hdf5.T_NATIVE_INT)
	require.Error(t, err)
	require.EqualError(t, err, "name ends with a slash")
}

func TestTouchGroup_CreateGroups(t *testing.T) {
	f, err := hdf5.OpenFile(tempFile(t), hdf5.F_ACC_CREAT+hdf5.F_ACC_RDWR)
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	defer f.Close()

	err = hdf5touch.TouchGroup(f, "a/b")

	if assert.NoError(t, err) {
		assert.True(t, f.LinkExists("a"))
		assert.True(t, f.LinkExists("a/b"))

		g, err := f.OpenGroup("a/b")
		assert.NoError(t, err)
		g.Close()
	}
}

func TestTouchGroup_OkWhenTheGroupExists(t *testing.T) {
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

func TestTouchGroup_ErrWhenFoundDatasets(t *testing.T) {
	f, err := hdf5.OpenFile(tempFile(t), hdf5.F_ACC_CREAT+hdf5.F_ACC_RDWR)
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	defer f.Close()

	dSpace, err := hdf5.CreateSimpleDataspace([]uint{1}, []uint{1})
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	defer dSpace.Close()
	dSet, err := f.CreateDataset("a", hdf5.T_NATIVE_INT, dSpace)
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	dSet.Close()

	err = hdf5touch.TouchGroup(f, "a/b")

	if assert.Error(t, err) {
		assert.EqualError(t, err, "is not a group: /a")
	}
}
