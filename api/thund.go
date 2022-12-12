package api

type Thund interface {
	Deploy(DAG) error
	Start() error
}
