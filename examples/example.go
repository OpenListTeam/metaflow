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
