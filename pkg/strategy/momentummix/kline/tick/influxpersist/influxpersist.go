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

var Bucket = "ticks_bucket"

type Client struct {
	Symbol           string
	Exchange         types.ExchangeName
	client           influxdb2.Client
	writeAPI         api.WriteAPI
	writeAPIBlocking api.WriteAPIBlocking
	Org              string
	Bucket           string
}

func NewClient(url, token, org, bucket string, symbol string, exchange types.ExchangeName) (*Client, error) {
	client := influxdb2.NewClient(url, token)
	writeAPI := client.WriteAPI(org, bucket)
	writeAPIBlocking := client.WriteAPIBlocking(org, bucket)

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
		Org:              org,
		Bucket:           bucket,
	}
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

//type BookTicker struct {
//	Time     time.Time
//	Symbol   string
//	Buy      fixedpoint.Value // `buy` from Max, `bidPrice` from binance
//	BuySize  fixedpoint.Value
//	Sell     fixedpoint.Value // `sell` from Max, `askPrice` from binance
//	SellSize fixedpoint.Value
//	//Last     fixedpoint.Value
//
//	// Fields below only exist in the futures book ticker event
//	TransactionTime time.Time
//}

func (c *Client) AppendTickBlocking(tick *types.BookTicker) {
	p := influxdb2.NewPointWithMeasurement(fmt.Sprintf("%s_%s", c.Symbol, c.Exchange)).
		AddField("buy", int64(tick.Buy)).
		AddField("buy_size", int64(tick.BuySize)).
		AddField("sell", int64(tick.Sell)).
		AddField("sell_size", int64(tick.SellSize)).
		AddTag("tick_time", strconv.FormatInt(tick.Time.UnixNano(), 10)).
		AddTag("transaction_time", strconv.FormatInt(tick.TransactionTime.UnixNano(), 10)).
		SetTime(tick.TransactionTime)
	err := c.writeAPIBlocking.WritePoint(context.Background(), p)
	if err != nil {
		fmt.Println("failed to write point:", err)
	}
	err = c.writeAPIBlocking.Flush(context.Background())
	if err != nil {
		fmt.Println("failed to flush:", err)
	}
}

func (c *Client) AppendTick(tick *types.BookTicker) {
	p := influxdb2.NewPointWithMeasurement(fmt.Sprintf("%s_%s", c.Symbol, c.Exchange)).
		AddField("buy", int64(tick.Buy)).
		AddField("buy_size", int64(tick.BuySize)).
		AddField("sell", int64(tick.Sell)).
		AddField("sell_size", int64(tick.SellSize)).
		AddTag("tick_time", strconv.FormatInt(tick.Time.UnixNano(), 10)).
		AddTag("transaction_time", strconv.FormatInt(tick.TransactionTime.UnixNano(), 10)).
		AddTag("insert_time", strconv.FormatInt(time.Now().UnixNano(), 10)).
		SetTime(tick.TransactionTime)
	c.writeAPI.WritePoint(p)
}

func (c *Client) Get(from, to time.Time) ([]*types.BookTicker, error) {
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
	var ticks []*types.BookTicker
	for result.Next() {
		if result.Record().Measurement() == fmt.Sprintf("%s_%s", c.Symbol, c.Exchange) {
			tickTimeStr, ok := result.Record().ValueByKey("tick_time").(string)
			if !ok {
				return nil, fmt.Errorf("tick_time is not string")
			}
			if tickTimeStr == "" {
				return nil, fmt.Errorf("tick_time is empty")
			}
			tickTimeInt, err := strconv.ParseInt(tickTimeStr, 10, 64)
			if err != nil {
				return nil, errors.Wrap(err, "failed to parse tick_time")
			}
			seconds := tickTimeInt / 1e9
			nanos := tickTimeInt % 1e9
			tickTime := time.Unix(seconds, nanos)
			transactionTimeStr, ok := result.Record().ValueByKey("transaction_time").(string)
			if !ok {
				return nil, fmt.Errorf("transaction_time is not string")
			}
			if transactionTimeStr == "" {
				return nil, fmt.Errorf("transaction_time is empty")
			}
			transactionTimeInt, err := strconv.ParseInt(transactionTimeStr, 10, 64)
			if err != nil {
				return nil, errors.Wrap(err, "failed to parse transaction_time")
			}
			seconds = transactionTimeInt / 1e9
			nanos = transactionTimeInt % 1e9
			transactionTime := time.Unix(seconds, nanos)
			buy, ok := result.Record().ValueByKey("buy").(int64)
			if !ok {
				return nil, fmt.Errorf("buy is not int64")
			}
			buySize, ok := result.Record().ValueByKey("buy_size").(int64)
			if !ok {
				return nil, fmt.Errorf("buy_size is not int64")
			}
			sell, ok := result.Record().ValueByKey("sell").(int64)
			if !ok {
				return nil, fmt.Errorf("sell is not int64")
			}
			sellSize, ok := result.Record().ValueByKey("sell_size").(int64)
			if !ok {
				return nil, fmt.Errorf("sell_size is not int64")
			}
			tick := &types.BookTicker{
				Time:            tickTime,
				Symbol:          c.Symbol,
				Buy:             fixedpoint.Value(buy),
				BuySize:         fixedpoint.Value(buySize),
				Sell:            fixedpoint.Value(sell),
				SellSize:        fixedpoint.Value(sellSize),
				TransactionTime: transactionTime,
			}
			ticks = append(ticks, tick)
		}
	}
	if result.Err() != nil {
		return nil, result.Err()
	}
	return ticks, nil
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
	c.writeAPI.Flush()
	c.client.Close()
}
