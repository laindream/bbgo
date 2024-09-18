package factor

type FactorType string

const (
	// ImbalanceRateFactor is a factor that calculates the imbalance rate of the order book, which is the ratio of the
	// buy quantity / sell quantity in a time window.
	ImbalanceRateFactor FactorType = "ImbalanceRateFactor"
)
