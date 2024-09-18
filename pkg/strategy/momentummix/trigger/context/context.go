package context

import "time"

var SamplingTimeAnchors []SamplingTimeAnchor

type SamplingTimeAnchor struct {
	PreviousTimeDuration time.Duration
	Weight               float64
}

func init() {
	InitSamplingTimeAnchors()
}

func InitSamplingTimeAnchors() {
	startingDuration := 125 * time.Millisecond
	magnificationRate := 2.0
	depth := 16
	for i := 0; i < depth; i++ {
		SamplingTimeAnchors = append(SamplingTimeAnchors, SamplingTimeAnchor{
			PreviousTimeDuration: startingDuration,
			Weight:               8.0 / (8.0 + float64(i)),
		})
		startingDuration = time.Duration(float64(startingDuration) * magnificationRate)
	}
}

type Context struct {
	Symbol string
}
