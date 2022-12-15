package impl

import "github.com/ignalina/thund/api"

type ThundK8s struct {
	dagProcessor api.DAG
}

func (tl ThundK8s) Deploy(dag api.DAG,customParams interface{}) error {
	return nil
}

func (tl ThundK8s) Start() error {

	return nil
}
