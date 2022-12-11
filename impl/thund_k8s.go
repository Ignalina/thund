package impl

import "github.com/ignalina/thund/api"

type ThundK8s struct {
	dagProcessor api.DAG
}

func (tl ThundK8s) Deploy(api.DAG) error {
	return nil
}

func (tl ThundK8s) Start() error {

	return nil
}
