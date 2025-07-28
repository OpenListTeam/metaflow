package httpwithoutbuffer

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/OpenListTeam/metaflow"
)

// 定义不支持操作的异常
var ErrUnsupported = errors.New("unsupported operation")

// HTTP流处理器
type httpStreamProcessor struct {
	metadata   *metaflow.StreamMetadata
	reader     io.Reader
	writer     io.Writer
	httpClient *http.Client
	position   int64
	closed     bool
	mu         sync.RWMutex
	method     string        // HTTP方法
	respBody   io.ReadCloser // 保存响应体用于关闭
}

func init() {
	httpStreamCreator := func(meta *metaflow.StreamMetadata) (metaflow.Stream, error) {
		return newHTTPStreamProcessor(meta)
	}
	metaflow.RegisterFactoryBuilder("http", httpStreamCreator) // 修正原代码中的拼写错误 Regsitry->Registry
	metaflow.RegisterFactoryBuilder("https", httpStreamCreator)
}

// 创建新的HTTP流处理器
func newHTTPStreamProcessor(metadata *metaflow.StreamMetadata) (metaflow.Stream, error) {
	processor := &httpStreamProcessor{
		metadata:   metadata,
		httpClient: &http.Client{Timeout: 30 * time.Second},
		method:     "GET", // 默认方法
	}

	// 检查元数据中是否指定了HTTP方法
	if method, ok := metadata.Metadata["http-method"]; ok {
		processor.method = method
		delete(metadata.Metadata, "http-method") // 从元数据中移除，避免作为头信息
	}

	// 根据HTTP方法决定是读取还是写入
	if processor.method == "GET" || processor.method == "HEAD" {
		// 读取模式
		return processor.setupReadMode()
	} else {
		// 写入模式
		return processor.setupWriteMode()
	}
}

// 设置读取模式
func (p *httpStreamProcessor) setupReadMode() (metaflow.Stream, error) {
	req, err := http.NewRequest(p.method, p.metadata.URL, nil)
	if err != nil {
		return nil, fmt.Errorf("创建HTTP请求失败: %w", err)
	}

	// 添加扩展元数据中的HTTP头
	for key, value := range p.metadata.Metadata {
		req.Header.Add(key, value)
	}

	// 执行请求
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("执行HTTP请求失败: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		resp.Body.Close()
		return nil, fmt.Errorf("HTTP请求失败: %s", resp.Status)
	}

	// 更新元数据
	if p.metadata.Size == 0 && resp.ContentLength > 0 {
		p.metadata.Size = resp.ContentLength
	}

	p.reader = resp.Body
	p.respBody = resp.Body // 保存响应体用于后续关闭
	return p, nil
}

// 设置写入模式
func (p *httpStreamProcessor) setupWriteMode() (metaflow.Stream, error) {
	// 写入模式使用临时文件作为写入缓冲区
	tmpFile, err := os.CreateTemp("", "http-write-*")
	if err != nil {
		return nil, fmt.Errorf("创建临时文件失败: %w", err)
	}
	p.writer = tmpFile
	return p, nil
}

// Read 实现 io.Reader 接口
func (p *httpStreamProcessor) Read(b []byte) (int, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return 0, io.ErrClosedPipe
	}

	if p.reader == nil {
		return 0, ErrUnsupported
	}

	n, err := p.reader.Read(b)
	p.position += int64(n)
	return n, err
}

// ReadAt 不支持，直接抛出异常
func (p *httpStreamProcessor) ReadAt(b []byte, off int64) (int, error) {
	return 0, ErrUnsupported
}

// Write 实现 io.Writer 接口
func (p *httpStreamProcessor) Write(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return 0, io.ErrClosedPipe
	}

	if p.writer == nil {
		return 0, ErrUnsupported
	}

	n, err := p.writer.Write(b)
	p.position += int64(n)

	// 更新元数据大小
	if n > 0 && p.metadata.Size < p.position {
		p.metadata.Size = p.position
	}

	return n, err
}

// Close 实现 io.Closer 接口
func (p *httpStreamProcessor) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}

	p.closed = true
	var err error

	// 处理写入模式的请求发送
	if p.writer != nil {
		// 类型转换为临时文件
		tmpFile, ok := p.writer.(*os.File)
		if !ok {
			return ErrUnsupported
		}

		// 重置文件指针到开始位置
		_, err = tmpFile.Seek(0, io.SeekStart)
		if err != nil {
			return fmt.Errorf("重置文件指针失败: %w", err)
		}

		// 计算校验和（在请求前进行，避免文件被HTTP客户端关闭）
		hash := sha256.New()
		// 创建文件副本用于计算哈希，避免影响后续操作
		if _, err = io.Copy(hash, tmpFile); err != nil {
			return fmt.Errorf("计算校验和失败: %w", err)
		}
		p.metadata.Checksum = hex.EncodeToString(hash.Sum(nil))

		// 重置文件指针到开始位置（为HTTP请求做准备）
		_, err = tmpFile.Seek(0, io.SeekStart)
		if err != nil {
			return fmt.Errorf("重置文件指针失败: %w", err)
		}

		// 创建请求
		req, err := http.NewRequest(p.method, p.metadata.URL, tmpFile)
		if err != nil {
			return fmt.Errorf("创建HTTP请求失败: %w", err)
		}

		// 添加请求头
		for key, value := range p.metadata.Metadata {
			req.Header.Add(key, value)
		}
		req.ContentLength = p.metadata.Size

		// 执行请求
		resp, err := p.httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("执行HTTP请求失败: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("HTTP请求失败: %s", resp.Status)
		}

		// 关闭并删除临时文件
		tmpFile.Close()
		os.Remove(tmpFile.Name())
	}

	// 关闭读取模式的响应体
	if p.respBody != nil {
		if closeErr := p.respBody.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}

	return err
}

// Seek 不支持，直接抛出异常
func (p *httpStreamProcessor) Seek(offset int64, whence int) (int64, error) {
	return 0, ErrUnsupported
}

// GetMetadata 返回流的元数据
func (p *httpStreamProcessor) GetMetadata() *metaflow.StreamMetadata {
	return p.metadata
}

// Checksum 实现校验和计算
func (p *httpStreamProcessor) Checksum() (string, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return "", io.ErrClosedPipe
	}

	if p.reader == nil {
		return "", ErrUnsupported
	}

	// 读取模式下计算校验和（注意：流式读取无法重新读取，这里仅支持写入模式）
	if p.method == "GET" || p.method == "HEAD" {
		return "", ErrUnsupported
	}

	// 写入模式下计算校验和
	tmpFile, ok := p.writer.(*os.File)
	if !ok {
		return "", ErrUnsupported
	}

	// 保存当前位置
	pos, err := tmpFile.Seek(0, io.SeekCurrent)
	if err != nil {
		return "", err
	}
	defer tmpFile.Seek(pos, io.SeekStart)

	// 从头计算校验和
	if _, err = tmpFile.Seek(0, io.SeekStart); err != nil {
		return "", err
	}

	hash := sha256.New()
	if _, err = io.Copy(hash, tmpFile); err != nil {
		return "", err
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}

// PartialChecksum 不支持，直接抛出异常
func (p *httpStreamProcessor) PartialChecksum(offset, limit int64) (string, error) {
	return "", ErrUnsupported
}

// IsReadOnly 判断流是否为只读
func (p *httpStreamProcessor) IsReadOnly() bool {
	return p.writer == nil
}
