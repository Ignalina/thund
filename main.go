package main

import (
	"github.com/ignalina/thund/api"
	"github.com/ignalina/thund/bundledImpl"
	"github.com/ignalina/thund/impl"
)

func main() {
	var t api.Thund
	t = impl.ThundLocal{}

	m := make(map[string]int)

	nodes := []api.DAGNode{
		api.DAGNode{
			Name: "rclone",
			NodeProcessor: api.PipelineProcessor{
				ProcessorImpl: bundledImpl.NewRCLONE("rclone"),
				IOEventImpl:   []api.IOEvent{&bundledImpl.MetricsEmitEvent{}},
			},
		}, api.DAGNode{
			Name: "fileconvert",
			NodeProcessor: api.PipelineProcessor{
				ProcessorImpl: bundledImpl.NewHDFS("fileconvert"),
				IOEventImpl:   []api.IOEvent{&bundledImpl.MetricsEmitEvent{}},
			},
		}}

	dag := api.DAG{Nodes: nodes}

	//	var customParams interface{}

	t.Deploy(dag, m)
	t.Start()

}
