package impl

import "github.com/ignalina/thund/api"

type ThundLocal struct {
	dagProcessor api.DAG
}

func (tl ThundLocal) Deploy(api.DAG) error {

	return nil
}

func (tl ThundLocal) Start() error {

	return nil
}
