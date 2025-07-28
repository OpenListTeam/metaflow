package http

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/OpenListTeam/metaflow"
)

// HTTP流处理器
type httpStreamProcessor struct {
	metadata   *metaflow.StreamMetadata
	reader     io.Reader
	writer     io.Writer
	seekable   io.Seeker
	httpClient *http.Client
	position   int64
	closed     bool
	mu         sync.RWMutex
	buffer     *bytes.Buffer // 用于写入的缓冲区
	method     string        // HTTP方法
}

func init() {
	httpStreamCreator := func(meta *metaflow.StreamMetadata) (metaflow.Stream, error) {
		return newHTTPStreamProcessor(meta)
	}
	metaflow.RegsitryFactoryBuilder("http", httpStreamCreator)
	metaflow.RegsitryFactoryBuilder("https", httpStreamCreator)
}

// 创建新的HTTP流处理器
func newHTTPStreamProcessor(metadata *metaflow.StreamMetadata) (metaflow.Stream, error) {
	processor := &httpStreamProcessor{
		metadata:   metadata,
		httpClient: &http.Client{Timeout: 30 * time.Second},
		buffer:     bytes.NewBuffer(nil),
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

	// 对于可seek的响应，使用缓冲
	if p.metadata.Size > 0 {
		buffer := bytes.NewBuffer(make([]byte, 0, p.metadata.Size))
		_, err := io.Copy(buffer, resp.Body)
		resp.Body.Close()

		if err != nil {
			return nil, fmt.Errorf("读取HTTP内容失败: %w", err)
		}

		p.reader = bytes.NewReader(buffer.Bytes())
		p.seekable = bytes.NewReader(buffer.Bytes())
	}

	return p, nil
}

// 设置写入模式
func (p *httpStreamProcessor) setupWriteMode() (metaflow.Stream, error) {
	// 写入模式下，先将数据缓冲在内存中，Close时发送请求
	p.writer = p.buffer
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
		return 0, io.ErrNoProgress
	}

	n, err := p.reader.Read(b)
	p.position += int64(n)
	return n, err
}

// ReadAt 实现 io.ReaderAt 接口
func (p *httpStreamProcessor) ReadAt(b []byte, off int64) (int, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return 0, io.ErrClosedPipe
	}

	if p.seekable == nil {
		return 0, io.ErrNoProgress
	}

	// 保存当前位置
	curPos, err := p.seekable.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}

	// 移动到指定位置
	_, err = p.seekable.Seek(off, io.SeekStart)
	if err != nil {
		return 0, err
	}

	// 读取数据
	n, err := p.reader.Read(b)

	// 恢复到之前的位置
	_, err2 := p.seekable.Seek(curPos, io.SeekStart)
	if err2 != nil {
		return n, err2
	}

	return n, err
}

// Write 实现 io.Writer 接口
func (p *httpStreamProcessor) Write(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return 0, io.ErrClosedPipe
	}

	if p.writer == nil {
		return 0, fmt.Errorf("流不支持写入操作")
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

	// 如果是写入模式，发送HTTP请求
	if p.writer != nil && p.buffer.Len() > 0 {
		req, err := http.NewRequest(p.method, p.metadata.URL, p.buffer)
		if err != nil {
			return fmt.Errorf("创建HTTP请求失败: %w", err)
		}

		// 添加扩展元数据中的HTTP头
		for key, value := range p.metadata.Metadata {
			req.Header.Add(key, value)
		}

		// 添加内容长度
		req.ContentLength = int64(p.buffer.Len())

		// 执行请求
		resp, err := p.httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("执行HTTP请求失败: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("HTTP请求失败: %s", resp.Status)
		}

		// 计算并更新校验和
		hash := sha256.New()
		hash.Write(p.buffer.Bytes())
		p.metadata.Checksum = hex.EncodeToString(hash.Sum(nil))
	}

	if p.reader != nil {
		if raw, ok := p.reader.(io.Closer); ok {
			err = raw.Close()
		}
	}
	if p.writer != nil {
		if raw, ok := p.writer.(io.Closer); ok {
			err = raw.Close()
		}
		p.writer = nil
	}
	return err
}

// Seek 实现 io.Seeker 接口
func (p *httpStreamProcessor) Seek(offset int64, whence int) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return 0, io.ErrClosedPipe
	}

	if p.seekable == nil {
		if p.writer != nil {
			// 写入模式下，允许在缓冲区中seek
			return p.Seek(offset, whence)
		}
		return 0, io.ErrNoProgress
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
			return 0, fmt.Errorf("无法从文件末尾定位：未知大小")
		}
		newPos = p.metadata.Size + offset
	default:
		return 0, fmt.Errorf("无效的定位方式: %d", whence)
	}

	if newPos < 0 {
		return 0, fmt.Errorf("无效的偏移量: %d", newPos)
	}

	p.position, err = p.seekable.Seek(newPos, io.SeekStart)
	return p.position, err
}

// GetMetadata 返回流的元数据
func (p *httpStreamProcessor) GetMetadata() *metaflow.StreamMetadata {
	return p.metadata
}

// Checksum 返回流的校验和
func (p *httpStreamProcessor) Checksum() (string, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return "", io.ErrClosedPipe
	}

	if p.reader == nil && p.buffer == nil {
		return "", io.ErrNoProgress
	}

	// 对于读取流，使用reader计算校验和
	if p.reader != nil {
		// 保存当前位置
		curPos, err := p.seekable.Seek(0, io.SeekCurrent)
		if err != nil {
			return "", err
		}

		// 重置到开始位置
		_, err = p.seekable.Seek(0, io.SeekStart)
		if err != nil {
			return "", err
		}

		// 计算SHA-256校验和
		hash := sha256.New()
		_, err = io.Copy(hash, p.reader)
		if err != nil {
			// 恢复位置
			p.seekable.Seek(curPos, io.SeekStart)
			return "", err
		}

		checksum := hex.EncodeToString(hash.Sum(nil))

		// 恢复到之前的位置
		p.seekable.Seek(curPos, io.SeekStart)

		return checksum, nil
	}

	// 对于写入流，使用buffer计算校验和
	if p.buffer != nil {
		hash := sha256.New()
		hash.Write(p.buffer.Bytes())
		return hex.EncodeToString(hash.Sum(nil)), nil
	}

	return "", fmt.Errorf("无法计算校验和")
}

// PartialChecksum 计算部分内容的校验和
func (p *httpStreamProcessor) PartialChecksum(offset, limit int64) (string, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return "", io.ErrClosedPipe
	}

	if offset < 0 || limit <= 0 {
		return "", fmt.Errorf("无效的偏移量或限制: offset=%d, limit=%d", offset, limit)
	}

	// 保存当前位置
	curPos, err := p.seekable.Seek(0, io.SeekCurrent)
	if err != nil {
		return "", err
	}

	// 定位到指定偏移量
	_, err = p.seekable.Seek(offset, io.SeekStart)
	if err != nil {
		return "", err
	}

	// 读取指定长度的数据
	data := make([]byte, limit)
	n, err := p.reader.Read(data)
	if err != nil && err != io.EOF {
		return "", err
	}

	// 恢复到之前的位置
	p.seekable.Seek(curPos, io.SeekStart)

	if n < int(limit) {
		data = data[:n] // 截断到实际读取的长度
	}

	// 计算SHA-256校验和
	hash := sha256.New()
	hash.Write(data)

	return hex.EncodeToString(hash.Sum(nil)), nil
}

// IsReadOnly 判断流是否为只读
func (p *httpStreamProcessor) IsReadOnly() bool {
	return p.writer == nil
}
