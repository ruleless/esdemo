package mockdata

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/elastic/go-elasticsearch/v7"
)

// ImportTestData 导入测试数据到ES数据库中
func ImportTestData(ctx context.Context, indexName string, body io.Reader) error {
	type bulkResponse struct {
		Errors bool `json:"errors"`
		Items  []struct {
			Index struct {
				ID     string `json:"_id"`
				Result string `json:"result"`
				Status int    `json:"status"`
				Error  struct {
					Type   string `json:"type"`
					Reason string `json:"reason"`
					Cause  struct {
						Type   string `json:"type"`
						Reason string `json:"reason"`
					} `json:"caused_by"`
				} `json:"error"`
			} `json:"index"`
		} `json:"items"`
	}

	es, err := elasticsearch.NewDefaultClient()
	if err != nil {
		log.Fatalf("Error creating the client: %s", err)
	}

	res, err := es.Bulk(body, es.Bulk.WithContext(ctx), es.Bulk.WithIndex(indexName))
	if err != nil {
		log.Fatalf("Failed to indexing test data: %s", err)
		return err
	}
	defer res.Body.Close()

	// If the whole request failed, print error and mark all documents as failed
	if res.IsError() {
		var raw map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&raw); err != nil {
			log.Fatalf("Failed to to parse response body: %s", err)
			return err
		}

		reason := raw["error"].(map[string]interface{})["reason"]
		log.Printf("  Error: [%d] %s: %s",
			res.StatusCode,
			raw["error"].(map[string]interface{})["type"],
			reason,
		)
		return fmt.Errorf("Failed to indexing test data: %s", reason)
	}

	// A successful response might still contain errors for particular documents
	var blk *bulkResponse
	if err := json.NewDecoder(res.Body).Decode(&blk); err != nil {
		log.Fatalf("Failed to to parse response body: %s", err)
		return err
	}

	numIndexed, numErrors := 0, 0
	for _, d := range blk.Items {
		if d.Index.Status > 201 {
			numErrors++

			log.Printf("  Error: [%d]: %s: %s: %s: %s",
				d.Index.Status,
				d.Index.Error.Type,
				d.Index.Error.Reason,
				d.Index.Error.Cause.Type,
				d.Index.Error.Cause.Reason,
			)
		} else {
			numIndexed++
		}
	}

	if numErrors > 0 {
		return fmt.Errorf("%d documents indexed failed", numErrors)
	}

	log.Printf("total indexed %d documents in [%s]", numIndexed, indexName)
	return nil
}

// ClearTestData 删除测试数据
func ClearTestData(ctx context.Context, indices ...string) error {
	es, err := elasticsearch.NewDefaultClient()
	if err != nil {
		log.Fatalf("Error creating the client: %s", err)
	}

	res, err := es.Indices.Delete(indices, es.Indices.Delete.WithContext(ctx))
	if err != nil {
		log.Fatalf("Failed to delete test data: %s", err)
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		var raw map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&raw); err != nil {
			log.Fatalf("Failed to to parse response body: %s", err)
			return err
		}

		reason := raw["error"].(map[string]interface{})["reason"]
		log.Printf("  Error: [%d] %s: %s",
			res.StatusCode,
			raw["error"].(map[string]interface{})["type"],
			reason,
		)
		return fmt.Errorf("Failed to delete test data: %s", reason)
	}

	log.Printf("Index: [%s] deleted", strings.Join(indices, ","))
	return nil
}
