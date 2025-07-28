# metaflow
一款简单高效的多源数据流处理工具
## 使用说明
``` go
package main

import (
	"github.com/OpenListTeam/metaflow"
	_ "github.com/OpenListTeam/metaflow/file"
)

func main() {
	streamMetadata := &metaflow.StreamMetadata{
		URL: "file://temp1/tmep2/test.txt",
	}
	content := []byte("Hello, Metaflow!")
	write(streamMetadata, content)
	result := readAll(streamMetadata)
	println(string(result))
}

func readAll(streamMetadata *metaflow.StreamMetadata) []byte {
	streamFlow, err := metaflow.CreateStream(streamMetadata)
	if err != nil {
		panic(err)
	}
	defer streamFlow.Close()

	content := make([]byte, streamMetadata.Size)
	_, err = streamFlow.Read(content)
	if err != nil {
		panic(err)
	}
	return content
}

func write(streamMetadata *metaflow.StreamMetadata, content []byte) {
	streamFlow, err := metaflow.CreateStream(streamMetadata)
	if err != nil {
		panic(err)
	}
	defer streamFlow.Close()
	_, err = streamFlow.Write(content)
	if err != nil {
		panic(err)
	}
}
```
## 贡献
欢迎提交 PR 或 Issue，帮助我们改进 Metaflow。请确保您的代码遵循 Go 语言的编码规范，并添加必要的注释和文档。
### 示例
``` go
package xxx

import (
	"github.com/OpenListTeam/metaflow"
)

func init(){
	metaflow.RegsitryFactoryBuilder("xxx", func(meta *metaflow.StreamMetadata) (metaflow.Stream, error) {
		return newXXXStreamProcessor(meta)
	})
}

func newXXXStreamProcessor(metadata *metaflow.StreamMetadata) (metaflow.Stream, error) {
	processor := &xxxStreamProcessor{
		metadata: metadata,
	}
	return processor, nil
}

// xxxStreamProcessor 实现了 metaflow.Stream 接口
...
```