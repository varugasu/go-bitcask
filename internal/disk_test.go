package internal_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/varugasu/go-bitcask/internal"
)

type DiskTestSuite struct {
	suite.Suite
	tempDir string
}

func (suite *DiskTestSuite) SetupTest() {
	dir, err := os.MkdirTemp(os.TempDir(), "")
	require.NoError(suite.T(), err)

	suite.tempDir = dir

	err = os.WriteFile(filepath.Join(suite.tempDir, "1"), internal.SerializeEntry(&internal.Entry{
		Key:       []byte("foo"),
		Value:     []byte("bar"),
		Timestamp: 1609459200,
	}), 0o644)
	require.NoError(suite.T(), err)

	err = os.WriteFile(filepath.Join(suite.tempDir, "2"), internal.SerializeEntry(&internal.Entry{
		Key:       []byte("test"),
		Value:     []byte("a value"),
		Timestamp: 1609459201,
	}), 0o644)
	require.NoError(suite.T(), err)

	os.WriteFile(filepath.Join(suite.tempDir, "3"), internal.SerializeEntry(&internal.Entry{
		Key:       []byte("a key"),
		Value:     []byte("another value"),
		Timestamp: 1609459202,
	}), 0o644)
	require.NoError(suite.T(), err)
}

func (suite *DiskTestSuite) TestInitKeyDir() {
	disk, err := internal.NewDisk(suite.tempDir)
	require.NoError(suite.T(), err)

	keyDir, err := disk.InitKeyDir()
	require.NoError(suite.T(), err)

	require.Equal(suite.T(), 3, len(keyDir))
	require.Equal(suite.T(), internal.ValuePosition{
		FileId:    suite.tempDir + "/1",
		Size:      3,
		Position:  29,
		Timestamp: 1609459200,
	}, keyDir["foo"])

	require.Equal(suite.T(), internal.ValuePosition{
		FileId:    suite.tempDir + "/2",
		Size:      7,
		Position:  30,
		Timestamp: 1609459201,
	}, keyDir["test"])

	require.Equal(suite.T(), internal.ValuePosition{
		FileId:    suite.tempDir + "/3",
		Size:      13,
		Position:  31,
		Timestamp: 1609459202,
	}, keyDir["a key"])
}

func (suite *DiskTestSuite) TestRead() {
	disk, err := internal.NewDisk(suite.tempDir)
	require.NoError(suite.T(), err)

	keyDir, err := disk.InitKeyDir()
	require.NoError(suite.T(), err)

	value, err := disk.Read(keyDir["foo"])
	require.NoError(suite.T(), err)
	require.Equal(suite.T(), []byte("bar"), value)

	value, err = disk.Read(keyDir["test"])
	require.NoError(suite.T(), err)
	require.Equal(suite.T(), []byte("a value"), value)

	value, err = disk.Read(keyDir["a key"])
	require.NoError(suite.T(), err)
	require.Equal(suite.T(), []byte("another value"), value)
}

func (suite *DiskTestSuite) TestWrite() {
	disk, err := internal.NewDisk(suite.tempDir)
	require.NoError(suite.T(), err)

	_, err = disk.InitKeyDir()
	require.NoError(suite.T(), err)

	err = disk.Write(&internal.Entry{
		Key:       []byte("new key"),
		Value:     []byte("new value"),
		Timestamp: 1609459203,
	})
	require.NoError(suite.T(), err)

	value, err := disk.Read(internal.ValuePosition{
		FileId:    filepath.Join(disk.ActiveDataFile.Directory, disk.ActiveDataFile.Filename),
		Size:      9,
		Position:  33,
		Timestamp: 1609459203,
	})
	require.NoError(suite.T(), err)
	require.Equal(suite.T(), []byte("new value"), value)
}

func TestDiskTestSuite(t *testing.T) {
	suite.Run(t, new(DiskTestSuite))
}
