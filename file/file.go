package file

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sync"

	"github.com/OpenListTeam/metaflow"
)

// 文件流处理器
type fileStreamProcessor struct {
	metadata *metaflow.StreamMetadata
	file     *os.File
	position int64
	closed   bool
	mu       sync.RWMutex
}

func init() {
	metaflow.RegisterFactoryBuilder("file", func(meta *metaflow.StreamMetadata) (metaflow.Stream, error) {
		return newFileStreamProcessor(meta)
	})
}

// 创建新的文件流处理器
func newFileStreamProcessor(metadata *metaflow.StreamMetadata) (metaflow.Stream, error) {
	u, err := url.Parse(metadata.URL)
	if err != nil {
		return nil, fmt.Errorf("解析URL失败: %w", err)
	}

	// 提取文件路径
	filePath := u.Path
	if u.Host != "" && u.Scheme == "file" {
		// 处理 file://host/path 格式
		filePath = filepath.Join(u.Host, u.Path)
	}
	// 文件夹判断是否存在，如果不存在，则创建
	if dir := filepath.Dir(filePath); dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("创建目录失败: %w", err)
		}
	}

	// 检查文件是否存在
	_, err = os.Stat(filePath)
	if err == nil {
		// 文件存在，以只读模式打开
		file, err := os.Open(filePath)
		if err != nil {
			return nil, fmt.Errorf("打开文件失败: %w", err)
		}

		// 如果元数据中没有大小信息，尝试获取
		if metadata.Size == 0 {
			fileInfo, err := file.Stat()
			if err == nil {
				metadata.Size = fileInfo.Size()
			}
		}

		return &fileStreamProcessor{
			metadata: metadata,
			file:     file,
		}, nil
	} else if os.IsNotExist(err) {
		// 文件不存在，以写入模式创建
		file, err := os.Create(filePath)
		if err != nil {
			return nil, fmt.Errorf("创建文件失败: %w", err)
		}

		return &fileStreamProcessor{
			metadata: metadata,
			file:     file,
		}, nil
	} else {
		return nil, fmt.Errorf("检查文件状态失败: %w", err)
	}
}

// Read 实现 io.Reader 接口
func (p *fileStreamProcessor) Read(b []byte) (int, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return 0, io.ErrClosedPipe
	}

	n, err := p.file.Read(b)
	p.position += int64(n)
	return n, err
}

// ReadAt 实现 io.ReaderAt 接口
func (p *fileStreamProcessor) ReadAt(b []byte, off int64) (int, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return 0, io.ErrClosedPipe
	}

	return p.file.ReadAt(b, off)
}

// Write 实现 io.Writer 接口
func (p *fileStreamProcessor) Write(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return 0, io.ErrClosedPipe
	}

	n, err := p.file.Write(b)
	p.position += int64(n)

	// 更新元数据大小
	if n > 0 && p.metadata.Size < p.position {
		p.metadata.Size = p.position
	}

	return n, err
}

// Close 实现 io.Closer 接口
func (p *fileStreamProcessor) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}

	p.closed = true

	return p.file.Close()
}

// Seek 实现 io.Seeker 接口
func (p *fileStreamProcessor) Seek(offset int64, whence int) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return 0, io.ErrClosedPipe
	}

	var newPos int64
	var err error

	switch whence {
	case io.SeekStart:
		newPos = offset
	case io.SeekCurrent:
		newPos = p.position + offset
	case io.SeekEnd:
		if p.metadata.Size == 0 {
			// 尝试获取文件大小
			fileInfo, err := p.file.Stat()
			if err != nil {
				return 0, fmt.Errorf("获取文件大小失败: %w", err)
			}
			p.metadata.Size = fileInfo.Size()
		}
		newPos = p.metadata.Size + offset
	default:
		return 0, fmt.Errorf("无效的定位方式: %d", whence)
	}

	if newPos < 0 {
		return 0, fmt.Errorf("无效的偏移量: %d", newPos)
	}

	p.position, err = p.file.Seek(newPos, io.SeekStart)
	return p.position, err
}

// GetMetadata 获取元数据
func (p *fileStreamProcessor) GetMetadata() *metaflow.StreamMetadata {
	return p.metadata
}

// Checksum 计算流的校验和
func (p *fileStreamProcessor) Checksum() (string, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return "", io.ErrClosedPipe
	}

	// 保存当前位置
	curPos, err := p.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return "", err
	}

	// 重置到开始位置
	_, err = p.file.Seek(0, io.SeekStart)
	if err != nil {
		return "", err
	}

	// 计算SHA-256校验和
	hash := sha256.New()
	_, err = io.Copy(hash, p.file)
	if err != nil {
		// 恢复位置
		p.file.Seek(curPos, io.SeekStart)
		return "", err
	}

	checksum := hex.EncodeToString(hash.Sum(nil))

	// 恢复到之前的位置
	p.file.Seek(curPos, io.SeekStart)

	return checksum, nil
}

func (p *fileStreamProcessor) PartialChecksum(offset, limit int64) (string, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return "", io.ErrClosedPipe
	}

	if offset < 0 || limit <= 0 {
		return "", fmt.Errorf("无效的偏移量或限制: offset=%d, limit=%d", offset, limit)
	}

	// 保存当前位置
	curPos, err := p.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return "", err
	}

	// 定位到指定偏移量
	_, err = p.file.Seek(offset, io.SeekStart)
	if err != nil {
		return "", err
	}

	// 计算SHA-256校验和
	hash := sha256.New()
	n, err := io.CopyN(hash, p.file, limit)
	if err != nil && err != io.EOF {
		// 恢复位置
		p.file.Seek(curPos, io.SeekStart)
		return "", err
	}

	checksum := hex.EncodeToString(hash.Sum(nil))

	// 恢复到之前的位置
	p.file.Seek(curPos, io.SeekStart)

	if n < limit {
		return checksum, fmt.Errorf("读取的字节数少于限制: %d < %d", n, limit)
	}

	return checksum, nil
}

// IsReadOnly 判断流是否为只读
func (p *fileStreamProcessor) IsReadOnly() bool {
	// 检查文件权限
	fileInfo, err := p.file.Stat()
	if err != nil {
		return true
	}

	// 如果文件模式包含写权限，则不是只读
	return fileInfo.Mode()&0200 == 0
}
