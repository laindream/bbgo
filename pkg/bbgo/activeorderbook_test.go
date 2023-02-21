package bbgo

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/c9s/bbgo/pkg/fixedpoint"
	"github.com/c9s/bbgo/pkg/types"
)

func TestActiveOrderBook_pendingOrders(t *testing.T) {
	now := time.Now()
	ob := NewActiveOrderBook("")

	// if we received filled order first
	// should be added to pending orders
	ob.orderUpdateHandler(types.Order{
		OrderID: 99,
		SubmitOrder: types.SubmitOrder{
			Symbol:       "BTCUSDT",
			Side:         types.SideTypeBuy,
			Type:         types.OrderTypeLimit,
			Quantity:     number(0.01),
			Price:        number(19000.0),
			AveragePrice: fixedpoint.Zero,
			StopPrice:    fixedpoint.Zero,
		},
		Status:       types.OrderStatusFilled,
		CreationTime: types.Time(now),
		UpdateTime:   types.Time(now),
	})

	assert.Len(t, ob.pendingOrderUpdates.Orders(), 1)

	// should be added to pending orders
	ob.Add(types.Order{
		OrderID: 99,
		SubmitOrder: types.SubmitOrder{
			Symbol:       "BTCUSDT",
			Side:         types.SideTypeBuy,
			Type:         types.OrderTypeLimit,
			Quantity:     number(0.01),
			Price:        number(19000.0),
			AveragePrice: fixedpoint.Zero,
			StopPrice:    fixedpoint.Zero,
		},
		Status:       types.OrderStatusNew,
		CreationTime: types.Time(now),
		UpdateTime:   types.Time(now.Add(-time.Second)),
	})

	o99, ok := ob.Get(99)
	assert.True(t, ok)
	assert.Equal(t, types.OrderStatusFilled, o99.Status)
}