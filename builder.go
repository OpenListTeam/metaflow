package metaflow

import (
	"fmt"
	"net/url"
)

type CreateFunc func(meta *StreamMetadata) (Stream, error)

var (
	_streamFactoryBuilder map[string]CreateFunc = make(map[string]CreateFunc)
)

// 创建流处理器
func CreateStream(metadata *StreamMetadata) (Stream, error) {
	u, err := url.Parse(metadata.URL)
	if err != nil {
		return nil, fmt.Errorf("解析URL失败: %w", err)
	}
	if create, ok := _streamFactoryBuilder[u.Scheme]; ok {
		return create(metadata)
	} else {
		return nil, fmt.Errorf("unsupport schema: %s", u.Scheme)
	}
}

func RegisterFactoryBuilder(schema string, creator CreateFunc) error {
	if _, ok := _streamFactoryBuilder[schema]; ok {
		return fmt.Errorf("duplicate factory constructor found: %s", schema)
	}
	_streamFactoryBuilder[schema] = creator
	return nil
}
