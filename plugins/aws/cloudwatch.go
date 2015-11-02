package aws

import (
	"fmt"
	"time"

    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/influxdb/telegraf/plugins"
)

var Debug bool

type Metric struct {
	Region string
	MetricName string
	Namespace string
	Statistics []string
	Period int64
	Duration int64
	Unit string
	Dimensions map[string]string
}

type CloudWatch struct {
	Debug bool
	Metrics []Metric
}

func (cw *CloudWatch) Description() string {
    return "Pull metrics from AWS CloudWatch."
}

func (cw *CloudWatch) SampleConfig() string {
    return "ok = true # indicate if everything is fine"
}

func (cw *CloudWatch) Gather(acc plugins.Accumulator) error {

	Debug = cw.Debug

	for _, m := range cw.Metrics {
		m.PushMetrics(acc)
	}

    return nil
}

func printDebug(m ...interface{}) {
	if Debug {
		fmt.Println(m...)
	}
}

func convertDimensions(dims map[string]string) []*cloudwatch.Dimension {
	awsDims := make([]*cloudwatch.Dimension, len(dims))
	var i int
	for key, value := range dims {
		awsDims[i] = &cloudwatch.Dimension {
			Name: aws.String(key),
			Value: aws.String(value),
		}
		i++
	}
	return awsDims
}


func (m *Metric) PushMetrics(acc plugins.Accumulator) error {

	sess := session.New(&aws.Config{Region: aws.String(m.Region)})
	svc := cloudwatch.New(sess)

	params := &cloudwatch.GetMetricStatisticsInput{
		EndTime:    aws.Time(time.Now()),
		MetricName: aws.String(m.MetricName),
		Namespace:  aws.String(m.Namespace),
		Period:     aws.Int64(m.Period),
		StartTime:  aws.Time(time.Now().Add(-time.Duration(m.Duration)*time.Second)),
		Statistics: aws.StringSlice(m.Statistics),
		Dimensions: convertDimensions(m.Dimensions),
		Unit: aws.String(m.Unit),
	}

	printDebug(params)

	resp, err := svc.GetMetricStatistics(params)

	if err != nil {
		fmt.Println(err.Error())
		return err
	}

	for _, d := range resp.Datapoints {
		acc.Add(*resp.Label, *d.Average, m.Dimensions, *d.Timestamp)
	}

	printDebug(resp)

	return nil
}

func init() {
    plugins.Add("cloudwatch", func() plugins.Plugin { return &CloudWatch{} })
}
