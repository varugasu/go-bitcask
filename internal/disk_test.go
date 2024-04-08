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
}

func (suite *DiskTestSuite) TestInitKeyDir() {
	err := os.WriteFile(filepath.Join(suite.tempDir, "1"), internal.SerializeEntry(&internal.Entry{
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
		Timestamp: 1609459201,
	}), 0o644)
	require.NoError(suite.T(), err)

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
		Timestamp: 1609459201,
	}, keyDir["a key"])
}

func TestDiskTestSuite(t *testing.T) {
	suite.Run(t, new(DiskTestSuite))
}
