package types

import (
	"fmt"
	"github.com/c9s/bbgo/pkg/fixedpoint"
	"time"
)

// BookTicker time exists in ftx, not in binance
// last exists in ftx, not in binance
type BookTicker struct {
	Time     time.Time
	Symbol   string
	Buy      fixedpoint.Value // `buy` from Max, `bidPrice` from binance
	BuySize  fixedpoint.Value
	Sell     fixedpoint.Value // `sell` from Max, `askPrice` from binance
	SellSize fixedpoint.Value
	//Last     fixedpoint.Value

	// Fields below only exist in the futures book ticker event
	TransactionTime time.Time
}

func (b BookTicker) String() string {
	return fmt.Sprintf("BookTicker { Symbol: %s,Buy: %f , BuySize: %f, Sell: %f, SellSize :%f } ", b.Symbol, b.Buy.Float64(), b.BuySize.Float64(), b.Sell.Float64(), b.SellSize.Float64())
}
