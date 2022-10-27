/*
 * MIT No Attribution
 *
 * Copyright 2022 Rickard Ernst Björn Lundin (rickard@ignalina.dk)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package bundledImpl

import (
	"context"
	"fmt"
	"github.com/ignalina/thund/api"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/viper"
	"io"
	"log"
	"net/http"
	"sync"
)

type MetricsEmitEvent struct {
	Metrics   *Metrics
	WaitGroup *sync.WaitGroup
	ctx       context.Context
}

type Metrics struct {
	FilesMovedOK  prometheus.Counter
	FileSizeOK    prometheus.Counter
	LineCountOK   prometheus.Counter
	AllocMib      prometheus.Gauge
	TotalAllocMib prometheus.Gauge
	SysMib        prometheus.Gauge
	NumGC         prometheus.Gauge
}

func (mee *MetricsEmitEvent) Process(reader io.Reader, customParams interface{}) bool {

	fmt.Println("Metrics Emit process")

	fe := customParams.(*api.FileEntity)
	mee.Metrics.FilesMovedOK.Inc()

	mee.Metrics.LineCountOK.Add(float64(fe.Lines))
	mee.Metrics.FileSizeOK.Add(float64(fe.Size))

	return true
}

func (mee *MetricsEmitEvent) Setup(customParams interface{}) bool {
	fmt.Println("Metrics Emit setup")
	mee.WaitGroup = new(sync.WaitGroup)
	mee.WaitGroup.Add(2)

	// Start metric server in a non blocking mode.
	go func() {
		log.Print("Starting metrics handler")
		http.Handle("/metrics", promhttp.Handler())
		adress := viper.GetString("metricsemit.address") + ":" + viper.GetString("metricsemit.port")
		log.Print(adress)
		http.ListenAndServe(adress, nil)

		mee.WaitGroup.Done()
		log.Fatal(" Metrics handler has stopped serving metrics....")
	}()

	mee.ctx = context.Background()

	return true
}

/*
    Need a teardown function
    WaitGroup.Done()
	WaitGroup.Wait()
*/

func InitMetrics() Metrics {
	m := Metrics{}
	m.FilesMovedOK = promauto.NewCounter(prometheus.CounterOpts{
		Name: "FilesMovedOk",
		Help: "Amount of files successfully moved",
	})

	m.FileSizeOK = promauto.NewCounter(prometheus.CounterOpts{
		Name: "FileSizeOk",
		Help: "Amount of bytes successfully moved",
	})

	m.LineCountOK = promauto.NewCounter(prometheus.CounterOpts{
		Name: "LineCountOK",
		Help: "Amount of lines successfully moved",
	})

	m.AllocMib = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "allocMib",
		Help: "allocMib",
	})
	m.TotalAllocMib = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "totalAllocMib",
		Help: "totalAllocMib",
	})
	m.SysMib = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "sysMib",
		Help: "sysMib",
	})
	m.NumGC = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "numGC",
		Help: "numGC",
	})

	return m
}
