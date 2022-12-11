package api

type DAG struct {
	Nodes []DAGNode
}

type DAGNode struct {
	//	out               []DAGNode
	pipelineProcessor PipelineProcessor
}
