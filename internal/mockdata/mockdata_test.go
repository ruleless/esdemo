package mockdata

import (
	"context"
	"os"
	"testing"
)

func TestImportTestData(t *testing.T) {
	file, err := os.Open("./testdata/data.bulk.json")
	if err != nil {
		t.Fatalf("open test data file failed: %v", err)
	}

	ImportTestData(context.Background(), "test", file)
}
