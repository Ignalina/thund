package api

type Thund interface {
	Deploy(dag DAG, customParams interface{}) error
	Start() error
}
