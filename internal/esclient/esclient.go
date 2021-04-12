package esclient

import (
	"fmt"

	"github.com/elastic/go-elasticsearch/v7"
)

var esclient *elasticsearch.Client

// Init 初始化
func Init() {
	var err error
	esclient, err = elasticsearch.NewDefaultClient()
	if err != nil {
		panic(fmt.Sprintf("Create elasticsearch client failed: %v", err))
	}
}

// GetClient 获取ES客户端对象
func GetClient() *elasticsearch.Client {
	return esclient
}
