package influxpersist

import (
	"context"
	"fmt"
	"github.com/c9s/bbgo/pkg/fixedpoint"
	"github.com/c9s/bbgo/pkg/types"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/pkg/errors"
	"strconv"
	"strings"
	"time"
)

var Bucket = "agg_trades_bucket"

type Client struct {
	Symbol           string
	Exchange         types.ExchangeName
	client           influxdb2.Client
	writeAPI         api.WriteAPI
	writeAPIBlocking api.WriteAPIBlocking
	//tradeChan        chan *types.Trade
	//stopChan         chan struct{}
	Org    string
	Bucket string
}

func NewClient(url, token, org, bucket string, symbol string, exchange types.ExchangeName) (*Client, error) {
	client := influxdb2.NewClient(url, token)
	writeAPI := client.WriteAPI(org, bucket)
	writeAPIBlocking := client.WriteAPIBlocking(org, bucket)

	// Ensure the bucket exists
	ctx := context.Background()
	if err := setupBucket(ctx, client, org, bucket); err != nil {
		return nil, err
	}

	c := &Client{
		Symbol:           symbol,
		Exchange:         exchange,
		client:           client,
		writeAPI:         writeAPI,
		writeAPIBlocking: writeAPIBlocking,
		//tradeChan:        make(chan *types.Trade, 5000), // buffer size can be adjusted
		//stopChan:         make(chan struct{}),
		Org:    org,
		Bucket: bucket,
	}

	//go c.runConsumer()
	return c, nil
}

func setupBucket(ctx context.Context, client influxdb2.Client, org, bucketName string) error {

	orgAPI := client.OrganizationsAPI()

	orgDomain, err := orgAPI.FindOrganizationByName(ctx, org)
	isOrgNotFound := err != nil && strings.HasSuffix(err.Error(), "not found")
	if err != nil && !isOrgNotFound {
		fmt.Printf("Error finding organization(%s): %s\n", org, err)
		return err
	}
	if orgDomain == nil {
		orgDomain, err = orgAPI.CreateOrganizationWithName(ctx, org)
		if err != nil {
			fmt.Printf("Error creating organization(%s): %s\n", org, err)
			return err
		}
	}

	bucketAPI := client.BucketsAPI()

	bucket, err := bucketAPI.FindBucketByName(ctx, bucketName)
	isBucketNotFound := err != nil && strings.HasSuffix(err.Error(), "not found")
	if err != nil && !isBucketNotFound {
		fmt.Printf("Error finding bucket(%s): %s\n", bucketName, err)
		return err
	}
	if bucket != nil {
		return nil
	}

	_, err = bucketAPI.CreateBucketWithName(ctx, orgDomain, bucketName)
	if err != nil {
		fmt.Printf("Error creating bucket(%s): %s\n", bucketName, err)
		return err
	}
	fmt.Println("Bucket created:", bucketName)
	return nil
}

//func (c *Client) runConsumer() {
//	for {
//		select {
//		case trade := <-c.tradeChan:
//			p := influxdb2.NewPointWithMeasurement(fmt.Sprintf("%s_%s", c.Symbol, c.Exchange)).
//				AddField("id", trade.ID).
//				AddField("price", int64(trade.Price)).
//				AddField("quantity", int64(trade.Quantity)).
//				AddField("quote_quantity", int64(trade.QuoteQuantity)).
//				AddTag("is_maker", fmt.Sprintf("%t", trade.IsMaker)).
//				SetTime(time.Time(trade.Time))
//			c.writeAPI.WritePoint(p)
//			c.writeAPI.Flush()
//		case <-c.stopChan:
//			c.writeAPI.Flush()
//			return
//		}
//	}
//}

func (c *Client) AppendTradeBlocking(trade *types.Trade) {
	p := influxdb2.NewPointWithMeasurement(fmt.Sprintf("%s_%s", c.Symbol, c.Exchange)).
		//AddField("id", trade.ID).
		AddField("price", int64(trade.Price)).
		AddField("quantity", int64(trade.Quantity)).
		AddField("quote_quantity", int64(trade.QuoteQuantity)).
		AddField("is_maker", trade.IsMaker).
		AddTag("trade_id", strconv.FormatUint(trade.ID, 10)).
		AddTag("trade_time", strconv.FormatInt(time.Time(trade.Time).UnixNano(), 10)).
		SetTime(time.Time(trade.Time))
	err := c.writeAPIBlocking.WritePoint(context.Background(), p)
	if err != nil {
		fmt.Println("failed to write point:", err)
	}
	err = c.writeAPIBlocking.Flush(context.Background())
	if err != nil {
		fmt.Println("failed to flush:", err)
	}
}

func (c *Client) AppendTrade(trade *types.Trade) {
	p := influxdb2.NewPointWithMeasurement(fmt.Sprintf("%s_%s", c.Symbol, c.Exchange)).
		//AddField("id", trade.ID).
		AddField("price", int64(trade.Price)).
		AddField("quantity", int64(trade.Quantity)).
		AddField("quote_quantity", int64(trade.QuoteQuantity)).
		AddField("is_maker", trade.IsMaker).
		AddTag("trade_id", strconv.FormatUint(trade.ID, 10)).
		AddTag("trade_time", strconv.FormatInt(time.Time(trade.Time).UnixNano(), 10)).
		SetTime(time.Time(trade.Time))
	c.writeAPI.WritePoint(p)
}

func (c *Client) Get(from, to time.Time) ([]*types.Trade, error) {
	queryAPI := c.client.QueryAPI(c.Org)
	fFrom := from.Format("2006-01-02T15:04:05.000Z")
	fTo := to.Format("2006-01-02T15:04:05.000Z")
	query := fmt.Sprintf(`from(bucket: "%s")
	|> range(start: %s, stop: %s)
	|> filter(fn: (r) => r._measurement == "%s_%s")
	|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")`,
		c.Bucket, fFrom, fTo, c.Symbol, c.Exchange)
	// Execute query
	result, err := queryAPI.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	var trades []*types.Trade
	for result.Next() {
		if result.Record().Measurement() == fmt.Sprintf("%s_%s", c.Symbol, c.Exchange) {
			tradeTimeStr, ok := result.Record().ValueByKey("trade_time").(string)
			if !ok {
				return nil, fmt.Errorf("trade_time is not string")
			}
			if tradeTimeStr == "" {
				return nil, fmt.Errorf("trade_time is empty")
			}
			tradeTimeInt, err := strconv.ParseInt(tradeTimeStr, 10, 64)
			if err != nil {
				return nil, errors.Wrap(err, "failed to parse trade_time")
			}
			seconds := tradeTimeInt / 1e9
			nanos := tradeTimeInt % 1e9
			tradeTime := time.Unix(seconds, nanos)
			isMaker, ok := result.Record().ValueByKey("is_maker").(bool)
			if !ok {
				return nil, fmt.Errorf("is_maker is not bool")
			}
			var side types.SideType
			var isBuyer bool
			if isMaker {
				side = types.SideTypeSell
				isBuyer = false
			} else {
				side = types.SideTypeBuy
				isBuyer = true
			}
			tradeIDStr, ok := result.Record().ValueByKey("trade_id").(string)
			if !ok {
				return nil, fmt.Errorf("trade_id is not string")
			}
			if tradeIDStr == "" {
				return nil, fmt.Errorf("trade_id is empty")
			}
			id, err := strconv.ParseUint(tradeIDStr, 10, 64)
			if err != nil {
				return nil, errors.Wrap(err, "failed to parse trade id")
			}
			price, ok := result.Record().ValueByKey("price").(int64)
			if !ok {
				return nil, fmt.Errorf("price is not int64")
			}
			quantity, ok := result.Record().ValueByKey("quantity").(int64)
			if !ok {
				return nil, fmt.Errorf("quantity is not int64")
			}
			quoteQuantity, ok := result.Record().ValueByKey("quote_quantity").(int64)
			if !ok {
				return nil, fmt.Errorf("quote_quantity is not int64")
			}
			trade := &types.Trade{
				ID:            id,
				Symbol:        c.Symbol,
				Exchange:      c.Exchange,
				Price:         fixedpoint.Value(price),
				Quantity:      fixedpoint.Value(quantity),
				QuoteQuantity: fixedpoint.Value(quoteQuantity),
				Time:          types.Time(tradeTime),
				IsMaker:       isMaker,
				Side:          side,
				IsBuyer:       isBuyer,
				OrderID:       0,
				Fee:           fixedpoint.Zero,
				FeeCurrency:   "",
			}
			trades = append(trades, trade)
		}
	}
	if result.Err() != nil {
		return nil, result.Err()
	}
	return trades, nil
}

func (c *Client) FlushBlocking() {
	err := c.writeAPIBlocking.Flush(context.Background())
	if err != nil {
		fmt.Printf("[%s][%s] failed to flush: %v\n", c.Symbol, c.Exchange, err)
	}
}

func (c *Client) Flush() {
	c.writeAPI.Flush()
}

func (c *Client) Close() {
	//close(c.stopChan)
	c.writeAPI.Flush()
	//err := c.writeAPIBlocking.Flush(context.Background())
	//if err != nil {
	//	return
	//}
	c.client.Close()
}
