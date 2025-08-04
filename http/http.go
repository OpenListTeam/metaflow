package http

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/OpenListTeam/metaflow"
)

var (
	ErrUnsupported   = errors.New("unsupported operation")
	ErrAlreadyClosed = errors.New("stream already closed")
	ErrWriteAfterEOF = errors.New("write after EOF")
)

type httpStreamProcessor struct {
	metadata   *metaflow.StreamMetadata
	reader     io.Reader // 读取模式：HTTP响应体
	writer     io.Writer // 写入模式：管道写端（用于流式上传）
	httpClient *http.Client
	position   int64
	closed     bool
	mu         sync.RWMutex
	method     string
	respBody   io.ReadCloser  // 读取模式：响应体（用于关闭）
	pipeReader *io.PipeReader // 写入模式：管道读端（绑定到HTTP请求体）
	pipeWriter *io.PipeWriter // 写入模式：管道写端（供用户写入）
	hash       hash.Hash      // 校验和计算器（延迟初始化）
	reqStarted bool           // 标记请求是否已发起
	reqDone    bool           // 标记请求是否已完成（写入模式）
	err        string         // 错误信息（写入模式）
}

func init() {
	httpStreamCreator := func(meta *metaflow.StreamMetadata) (metaflow.Stream, error) {
		return newHTTPStreamProcessor(meta)
	}
	metaflow.RegisterFactoryBuilder("http", httpStreamCreator)
	metaflow.RegisterFactoryBuilder("https", httpStreamCreator)
}

func newHTTPStreamProcessor(metadata *metaflow.StreamMetadata) (metaflow.Stream, error) {
	processor := &httpStreamProcessor{
		metadata:   metadata,
		httpClient: &http.Client{Timeout: 60 * time.Second},
		method:     "GET",
	}

	if method, ok := metadata.Metadata["http-method"]; ok {
		processor.method = method
		delete(metadata.Metadata, "http-method")
	}

	// 写入模式：初始化管道（不提前发起请求，等待首次Write）
	if processor.method != "GET" && processor.method != "HEAD" {
		pr, pw := io.Pipe()
		processor.pipeReader = pr
		processor.pipeWriter = pw
		processor.writer = pw // 初始writer指向管道写端
	}

	return processor, nil
}

// Read：首次调用时发起GET请求，直接读取响应体
func (p *httpStreamProcessor) Read(b []byte) (int, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return 0, ErrAlreadyClosed
	}
	p.mu.RUnlock()

	// 首次Read时发起GET请求
	if !p.reqStarted {
		p.mu.Lock()
		if p.reqStarted { // 双重检查避免并发
			p.mu.Unlock()
			return 0, nil
		}

		// 创建并执行GET请求
		req, err := http.NewRequest("GET", p.metadata.URL, nil)
		if err != nil {
			p.mu.Unlock()
			return 0, fmt.Errorf("创建GET请求失败: %w", err)
		}
		for key, value := range p.metadata.Metadata {
			req.Header.Add(key, value)
		}

		resp, err := p.httpClient.Do(req)
		if err != nil {
			p.mu.Unlock()
			return 0, fmt.Errorf("执行GET请求失败: %w", err)
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			resp.Body.Close()
			p.mu.Unlock()
			return 0, fmt.Errorf("GET请求失败: %s", resp.Status)
		}

		// 绑定响应体
		p.reader = resp.Body
		p.respBody = resp.Body
		p.metadata.Size = resp.ContentLength
		p.reqStarted = true
		p.mu.Unlock()
	}

	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.reader == nil {
		return 0, ErrUnsupported
	}

	// 读取数据
	n, err := p.reader.Read(b)
	p.position += int64(n)

	// 延迟初始化hash并更新
	if p.hash == nil && n > 0 {
		p.hash = sha256.New()
	}
	if p.hash != nil && n > 0 {
		_, _ = p.hash.Write(b[:n])
	}

	// 读取完成时计算校验和
	if err == io.EOF && p.hash != nil {
		p.metadata.Checksum = hex.EncodeToString(p.hash.Sum(nil))
	}

	return n, err
}

// Write：首次调用时发起PUT请求，后续写入数据直接发送到服务器（流式上传）
func (p *httpStreamProcessor) Write(b []byte) (int, error) {
	p.mu.RLock()
	// 检查状态：已关闭或请求已完成则拒绝写入
	if p.closed || p.reqDone {
		p.mu.RUnlock()
		return 0, ErrWriteAfterEOF
	}
	p.mu.RUnlock()

	// 首次Write时发起PUT请求（同步）
	if !p.reqStarted {
		p.mu.Lock()
		if p.reqStarted { // 双重检查避免并发
			p.mu.Unlock()
			return 0, nil
		}

		// 延迟初始化hash
		if len(b) > 0 {
			p.hash = sha256.New()
		}

		// 创建PUT请求，以管道读端作为请求体（流式上传）
		req, err := http.NewRequest(p.method, p.metadata.URL, p.pipeReader)
		if err != nil {
			p.mu.Unlock()
			return 0, fmt.Errorf("创建PUT请求失败: %w", err)
		}
		for key, value := range p.metadata.Metadata {
			req.Header.Add(key, value)
		}
		// 不设置ContentLength，使用分块传输（Transfer-Encoding: chunked）

		// 同步执行请求（在单独goroutine中等待响应，避免阻塞写入）
		go func() {
			resp, err := p.httpClient.Do(req)
			if err != nil {
				p.mu.Lock()
				p.err = fmt.Sprintf("PUT请求失败: %v", err)
				p.mu.Unlock()
				return
			}
			defer resp.Body.Close()

			// 读取响应体确保服务器处理完成
			_, _ = io.ReadAll(resp.Body)

			// 标记请求完成
			p.mu.Lock()
			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				p.err = fmt.Sprintf("PUT请求失败: %s", resp.Status)
			}
			p.reqDone = true
			p.mu.Unlock()
		}()

		p.reqStarted = true
		p.mu.Unlock()
	}

	p.mu.RLock()
	defer p.mu.RUnlock()

	// 写入数据到管道（直接发送到服务器）
	n, err := p.writer.Write(b)
	p.position += int64(n)

	// 更新hash
	if p.hash != nil && n > 0 {
		_, _ = p.hash.Write(b[:n])
	}

	// 更新元数据大小
	if p.metadata.Size < p.position {
		p.metadata.Size = p.position
	}

	// 如果写入完成（如len(b)=0或遇到错误），关闭管道写端
	if err != nil || n == 0 {
		p.pipeWriter.Close()
		p.reqDone = true
	}

	// 全部数据写入完成后计算校验和
	if p.reqDone && p.hash != nil {
		p.metadata.Checksum = hex.EncodeToString(p.hash.Sum(nil))
	}

	return n, err
}

// Close：仅清理资源，不处理请求逻辑（请求由Write触发并完成）
func (p *httpStreamProcessor) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}
	p.closed = true

	var err error

	// 写入模式：关闭管道
	if p.pipeWriter != nil {
		if closeErr := p.pipeWriter.Close(); closeErr != nil {
			err = fmt.Errorf("关闭管道失败: %w", closeErr)
		}
	}

	// 读取模式：关闭响应体
	if p.respBody != nil {
		if closeErr := p.respBody.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}

	return err
}

// 其他方法保持不变
func (p *httpStreamProcessor) ReadAt(b []byte, off int64) (int, error) {
	return 0, ErrUnsupported
}

func (p *httpStreamProcessor) Seek(offset int64, whence int) (int64, error) {
	return 0, ErrUnsupported
}

func (p *httpStreamProcessor) GetMetadata() *metaflow.StreamMetadata {
	return p.metadata
}

func (p *httpStreamProcessor) Checksum() (string, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return "", ErrAlreadyClosed
	}

	if p.hash == nil {
		return "", nil
	}

	return hex.EncodeToString(p.hash.Sum(nil)), nil
}

func (p *httpStreamProcessor) PartialChecksum(offset, limit int64) (string, error) {
	return "", ErrUnsupported
}

func (p *httpStreamProcessor) IsReadOnly() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.writer == nil
}
