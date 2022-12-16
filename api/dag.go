package api

type DAG struct {
	Nodes []DAGNode
}

type DAGNode struct {
	//	out               []DAGNode
	Name          string
	NodeProcessor NodeProcessor
}
