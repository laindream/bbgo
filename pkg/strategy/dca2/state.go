package dca2

import (
	"context"
	"time"
)

type State int64

const (
	None State = iota
	WaitToOpenPosition
	OpenPositionReady
	OpenPositionOrderFilled
	OpenPositionOrdersCancelled
	TakeProfitReady
	TakeProfitOrderFilled
)

// runState
// WaitToOpenPosition -> after startTimeOfNextRound, place dca orders ->
// OpenPositionReady -> any dca maker order filled ->
// OpenPositionOrderFilled -> price hit the take profit ration, start cancelling ->
// OpenPositionOrdersCancelled -> place the takeProfit order ->
// TakeProfitReady -> the takeProfit order filled ->
// WaitToOpenPosition
func (s *Strategy) runState(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			s.logger.Info("[DCA] runState DONE")
			return
		case nextState := <-s.nextStateC:
			s.logger.Infof("[DCA] currenct state: %d, next state: %d", s.state, nextState)
			switch s.state {
			case None:
				if nextState != WaitToOpenPosition {
					continue
				}
				s.state = WaitToOpenPosition
			case WaitToOpenPosition:
				if nextState != OpenPositionReady {
					continue
				}

				if time.Now().Before(s.startTimeOfNextRound) {
					continue
				}

				if err := s.placeOpenPositionOrders(ctx); err != nil {
					s.logger.WithError(err).Error("failed to place dca orders, please check it.")
					continue
				}
				s.state = OpenPositionReady
			case OpenPositionReady:
				if nextState != OpenPositionOrderFilled {
					continue
				}
				s.state = OpenPositionOrderFilled
			case OpenPositionOrderFilled:
				if nextState != OpenPositionOrdersCancelled {
					continue
				}
				if err := s.cancelOpenPositionOrders(ctx); err != nil {
					s.logger.WithError(err).Error("failed to cancel maker orders")
					continue
				}
				s.state = OpenPositionOrdersCancelled
				s.nextStateC <- TakeProfitReady
			case OpenPositionOrdersCancelled:
				if nextState != TakeProfitReady {
					continue
				}
				if err := s.placeTakeProfitOrders(ctx); err != nil {
					s.logger.WithError(err).Error("failed to open take profit orders")
					continue
				}
				s.state = TakeProfitReady
			case TakeProfitReady:
				if nextState != TakeProfitOrderFilled {
					continue
				}
				if s.Short {
					s.Budget = s.Budget.Add(s.Position.Base)
				} else {
					s.Budget = s.Budget.Add(s.Position.Quote)
				}

				// reset position
				s.Position.Reset()
				s.state = TakeProfitOrderFilled
				s.nextStateC <- WaitToOpenPosition
			case TakeProfitOrderFilled:
				if nextState != WaitToOpenPosition {
					continue
				}

				// set the start time of the next round
				s.startTimeOfNextRound = time.Now().Add(s.CoolDownInterval.Duration())
				s.state = WaitToOpenPosition
			}
		}
	}
}
