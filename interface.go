package metaflow

import (
	"io"
	"time"
)

// 流元数据结构
type StreamMetadata struct {
	URL       string            `json:"url"`                  // 资源URL
	Size      int64             `json:"size,omitempty"`       // 流大小（可选）
	Checksum  string            `json:"checksum,omitempty"`   // 内容校验和（可选）
	ExpiresAt time.Time         `json:"expires_at,omitempty"` // 过期时间（可选）
	Metadata  map[string]string `json:"metadata,omitempty"`   // 扩展元数据
}

// 统一的流处理器接口
type Stream interface {
	io.Reader
	io.ReaderAt
	io.Writer
	io.Closer
	io.Seeker

	GetMetadata() *StreamMetadata                        // 获取元数据
	Checksum() (string, error)                           // 计算内容校验和
	PartialChecksum(offset, limit int64) (string, error) // 计算内容校验和
	IsReadOnly() bool                                    // 判断是否为只读流
}
