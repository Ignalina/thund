package api

type Thund interface {
	Deploy(DAG) error
	start() error
}
