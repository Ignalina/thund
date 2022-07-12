package api

type FrontAgent struct {
	IOEventImpl IOEvent
	WatcherImpl Watcher
	// todo more...
}

func (fa FrontAgent) Start() {
	fa.WatcherImpl.Watch(fa.IOEventImpl)

}
