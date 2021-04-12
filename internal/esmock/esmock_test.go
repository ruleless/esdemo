package mockdata

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"testing"

	"esdemo/internal/esclient"
)

func TestImportTestData(t *testing.T) {
	esclient.Init()

	file, err := os.Open("./testdata/data.bulk.json")
	if err != nil {
		t.Fatalf("open test data file failed: %v", err)
	}

	ctx := context.Background()
	ImportTestData(ctx, "test", file)

	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match_all": map[string]interface{}{},
		},
	}
	es := esclient.GetClient()
	resp, err := esclient.WrapSearch(ctx, query,
		es.Search.WithIndex("test"),
	)
	if err != nil {
		t.Errorf("search failed: %v", err)
	} else {
		log.Printf("Got %d result", resp.Hits.Total.Value)
		for _, hit := range resp.Hits.Hits {
			b, err := json.Marshal(hit.Source)
			if err != nil {
				t.Errorf("Marshal failed: %v", err)
			} else {
				log.Printf("doc ID: %s, doc: %v", hit.ID, string(b))
			}
		}
	}

	ClearTestData(ctx, "test")
}
