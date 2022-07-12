package api

type Watcher interface {
	Watch(event IOEvent) bool
}
