// Copyright 2012 Samuel Stauffer. All rights reserved.
// Use of this source code is governed by a 3-clause BSD
// license that can be found in the LICENSE file.

package metrics

import (
	"container/heap"
	"math"
	"math/rand"
	"time"
)

const (
	edRescaleThreshold = time.Hour
)

// Reservoir

type priorityValue struct {
	priority float64
	value    int64
}

type reservoir struct {
	samples []priorityValue
}

func (r *reservoir) Clear() {
	r.samples = r.samples[:0]
}

func (r *reservoir) Get(i int) priorityValue {
	return r.samples[i]
}

func (r *reservoir) Values() (values []int64) {
	values = make([]int64, len(r.samples))
	for i, sample := range r.samples {
		values[i] = sample.value
	}
	return
}

func (r *reservoir) ScalePriority(scale float64) {
	for i, sample := range r.samples {
		r.samples[i] = priorityValue{priority: sample.priority * scale, value: sample.value}
	}
}

func (r *reservoir) Len() int {
	return len(r.samples)
}

func (r *reservoir) Less(i, j int) bool {
	return r.samples[i].priority < r.samples[j].priority
}

func (r *reservoir) Swap(i, j int) {
	r.samples[i], r.samples[j] = r.samples[j], r.samples[i]
}

func (r *reservoir) Push(x interface{}) {
	r.samples = append(r.samples, x.(priorityValue))
}

func (r *reservoir) Pop() interface{} {
	v := r.samples[len(r.samples)-1]
	r.samples = r.samples[:len(r.samples)-1]
	return v
}

type exponentiallyDecayingSample struct {
	// the number of samples to keep in the sampling reservoir
	reservoirSize int
	// the exponential decay factor; the higher this is, the more
	// biased the sample will be towards newer values
	alpha         float64
	values        *reservoir
	startTime     time.Time
	nextScaleTime time.Time
	now           func() time.Time
}

// NewExponentiallyDecayingSample returns an exponentially-decaying random
// sample of values. Uses Cormode et al's forward-decaying priority reservoir
// sampling method to produce a statistically representative sample,
// exponentially biased towards newer entries.
//
// http://www.research.att.com/people/Cormode_Graham/library/publications/CormodeShkapenyukSrivastavaXu09.pdf
// Cormode et al. Forward Decay: A Practical Time Decay Model for Streaming
// Systems. ICDE '09: Proceedings of the 2009 IEEE International Conference on
// Data Engineering (2009)
func NewExponentiallyDecayingSample(reservoirSize int, alpha float64) Sample {
	return NewExponentiallyDecayingSampleWithCustomTime(reservoirSize, alpha, time.Now)
}

// NewExponentiallyDecayingSampleWithCustomTime returns an exponentially-decaying random
// sample of values using a custom time function.
func NewExponentiallyDecayingSampleWithCustomTime(reservoirSize int, alpha float64, now func() time.Time) Sample {
	eds := exponentiallyDecayingSample{
		reservoirSize: reservoirSize,
		alpha:         alpha,
		values: &reservoir{
			samples: make([]priorityValue, 0, reservoirSize),
		},
		now: now,
	}
	eds.Clear()
	return &eds
}

func (s *exponentiallyDecayingSample) Clear() {
	s.values.Clear()
	heap.Init(s.values)
	s.startTime = s.now()
	s.nextScaleTime = s.startTime.Add(edRescaleThreshold)
}

func (s *exponentiallyDecayingSample) Len() int {
	return s.values.Len()
}

func (s *exponentiallyDecayingSample) Values() []int64 {
	return s.values.Values()
}

func (s *exponentiallyDecayingSample) Update(value int64) {
	timestamp := s.now()
	if timestamp.After(s.nextScaleTime) {
		s.rescale(timestamp)
		timestamp = s.now()
	}

	priority := s.weight(timestamp.Sub(s.startTime)) / rand.Float64()
	if s.values.Len() < s.reservoirSize {
		heap.Push(s.values, priorityValue{priority: priority, value: value})
	} else {
		if first := s.values.Get(0); first.priority < priority {
			heap.Pop(s.values)
			heap.Push(s.values, priorityValue{priority: priority, value: value})
		}
	}
}

func (s *exponentiallyDecayingSample) weight(delta time.Duration) float64 {
	return math.Exp(s.alpha * delta.Seconds())
}

/*
A common feature of the above techniques—indeed, the key technique that
allows us to track the decayed weights efficiently—is that they maintain
counts and other quantities based on g(ti − L), and only scale by g(t − L)
at query time. But while g(ti −L)/g(t−L) is guaranteed to lie between zero
and one, the intermediate values of g(ti − L) could become very large. For
polynomial functions, these values should not grow too large, and should be
effectively represented in practice by floating point values without loss of
precision. For exponential functions, these values could grow quite large as
new values of (ti − L) become large, and potentially exceed the capacity of
common floating point types. However, since the values stored by the
algorithms are linear combinations of g values (scaled sums), they can be
rescaled relative to a new landmark. That is, by the analysis of exponential
decay in Section III-A, the choice of L does not affect the final result. We
can therefore multiply each value based on L by a factor of exp(−α(L′ − L)),
and obtain the correct value as if we had instead computed relative to a new
landmark L′ (and then use this new L′ at query time). This can be done with
a linear pass over whatever data structure is being used.
*/
func (s *exponentiallyDecayingSample) rescale(now time.Time) {
	s.nextScaleTime = now.Add(edRescaleThreshold)
	oldStartTime := s.startTime
	s.startTime = now
	scale := math.Exp(-s.alpha * s.startTime.Sub(oldStartTime).Seconds())
	s.values.ScalePriority(scale)
}
