package esclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/elastic/go-elasticsearch/v7/esapi"
)

// Hit 搜索命中的结果
type Hit struct {
	Index string `json:"_index"`
	Type  string `json:"_type"`
	ID    string `json:"_id"`

	// "_score"

	Source map[string]interface{} `json:"_source"`
}

// SearchResponse 搜索或聚合的结果
type SearchResponse struct {
	Took     float64 `json:"took"`
	TimedOut bool    `json:"timed_out"`

	Shards struct {
		Total      int64 `json:"total"`
		Successful int64 `json:"successful"`
		Skipped    int64 `json:"skipped"`
		Failed     int64 `json:"failed"`
	} `json:"_shards"`

	// 搜索结果
	Hits struct {
		Total struct {
			Value    int64  `json:"value"`
			Relation string `json:"relation"`
		} `json:"total"`
		// "max_score"

		Hits []Hit `json:"hits"`
	} `json:"hits"`

	// 聚合结果
	Aggs map[string]interface{} `json:"aggregations"`
}

// WrapSearch 封装go-elasticsearch的搜索函数
func WrapSearch(ctx context.Context, query map[string]interface{},
	o ...func(*esapi.SearchRequest)) (*SearchResponse, error) {

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		log.Printf("Error encoding query: %s", err)
		return nil, err
	}

	// Perform the search request.
	es := GetClient()
	searchOpts := []func(*esapi.SearchRequest){
		es.Search.WithContext(ctx),
		es.Search.WithBody(&buf),
		es.Search.WithTrackTotalHits(true),
	}
	searchOpts = append(searchOpts, o...)
	res, err := es.Search(searchOpts...)
	if err != nil {
		log.Printf("Error getting response: %s", err)
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		var e map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			log.Printf("Error parsing the response body: %s", err)
			return nil, err
		}

		// Print the response status and error information.
		reason := e["error"].(map[string]interface{})["reason"]
		log.Printf("[%s] %s: %s",
			res.Status(),
			e["error"].(map[string]interface{})["type"],
			reason,
		)
		return nil, fmt.Errorf("Search error: %s", reason)
	}

	var resp SearchResponse
	if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
		log.Printf("Error parsing the response body: %s", err)
		return nil, err
	}

	return &resp, nil
}
