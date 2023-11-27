package dca2

type State int64

const (
	WaitToOpenMaker State = iota + 1
	MakerReady
	MakerFilled
	MakerCancelled
	TakeProfitReady
)
