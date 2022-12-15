package impl

import (
	"github.com/ignalina/thund/api"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

// Dummy implementation missing everthing like Topological sort etc.

type ThundLocal struct {
	dagProcessor api.DAG
}

func (tl ThundLocal) Deploy(dag api.DAG,customParams interface{}) error {

	tl.dagProcessor = dag
	for _, dp := range tl.dagProcessor.Nodes {
		dp.PipelineProcessor.Setup(customParams)
	}

	return nil
}

func (tl ThundLocal) Start() error {
	stop := make(chan struct{})
	var wg *sync.WaitGroup
	HandleShutdown(stop)

	go tl.procesDag(wg, stop)

	wg.Wait()
	return nil
}

// Process DAG and listen to system shutdown
func (tl ThundLocal) procesDag(wg *sync.WaitGroup, stop chan struct{}) {
	for true {
		defer wg.Done()
		for _, dp := range tl.dagProcessor.Nodes {
			select {
			case <-stop:
				return
			default:
			}
			dp.PipelineProcessor.Process()
		}
		// todo check for signal for a nice takedown.
	}
	return
}

func HandleShutdown(stop chan struct{}) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)

	// Setup signal handlers
	go func() {
		<-c
		// This will send the stop signal to all goroutines
		close(stop)
	}()
}
