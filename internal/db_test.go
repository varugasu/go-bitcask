package internal_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/varugasu/go-bitcask/internal"
)

type DatabaseTestSuite struct {
	suite.Suite

	database *internal.Database
	tempDir  string
}

func (suite *DatabaseTestSuite) SetupTest() {
	dir, err := os.MkdirTemp(os.TempDir(), "")
	require.NoError(suite.T(), err)

	suite.tempDir = dir

	database, err := internal.NewDatabase(suite.tempDir)
	require.NoError(suite.T(), err)

	suite.database = database

	database.Put("key1", []byte("value1"))
	database.Put("key2", []byte("value2"))
	database.Put("key3", []byte("value3"))
}

func (suite *DatabaseTestSuite) TestDelete() {
	value, err := suite.database.Get("key1")
	require.NoError(suite.T(), err)

	require.Equal(suite.T(), []byte("value1"), value)

	suite.database.Delete("key1")
	_, err = suite.database.Get("key1")
	require.Error(suite.T(), err)
	require.Equal(suite.T(), "deleted", err.Error())
}

func TestDatabaseTestSuite(t *testing.T) {
	suite.Run(t, new(DatabaseTestSuite))
}
