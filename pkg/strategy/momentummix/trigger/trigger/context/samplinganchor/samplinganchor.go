package sampling_anchor

import "time"

type SamplingAnchor interface {
	GetPreviousTimeAnchor( int) time.Duration
	GetSamplingData()
}

type SamplingCalculator struct {

}

func (s *SamplingCalculator) GetPreviousTimeAnchor( i int) time.Duration {