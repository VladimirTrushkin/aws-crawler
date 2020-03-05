package main

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"regexp"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

// Item ...
type Item struct {
	URL   string `json:"url"`
	Count int    `json:"count"`
}

// StreamRecord ...
type StreamRecord struct {
	NewImage map[string]*dynamodb.AttributeValue `json:"NewImage"`
}

// Record ...
type Record struct {
	EventName string        `json:"eventName"`
	Dynamodb  *StreamRecord `json:"dynamodb"`
}

// Event ...
type Event struct {
	Records []*Record `json:"Records"`
}

// IMPORTANT! Stop criteria unless your Lambda will run
// until it downloads the entire Intrernet
// I using depth counter to stop after achieving depth = 4
const maxDepth int = 4

// HandleRequest ...
// for each record in event, downlooad url, then parce it to find more url.
// inssert new urls into dynamoDB to spawn more events
func HandleRequest(ctx context.Context, event Event) (string, error) {

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-east-1")},
	)
	if err != nil {
		return "", nil
	}

	for _, r := range event.Records {

		// ignore all events but INSERT
		if r.EventName != "INSERT" {
			continue
		}

		item := Item{}

		err := dynamodbattribute.UnmarshalMap(r.Dynamodb.NewImage, &item)
		if err != nil {
			return "", err
		}

		fmt.Println(item)

		// IMPORTANT! Stop criteria unless your Lambda will run
		// until it downloads the entire Intrernet
		// I using depth counter to stop after achieving depth = 4
		if item.Count >= maxDepth {
			break
		} else {
			item.Count++
		}

		res, err := geturl(item.URL)
		if err != nil {
			return "", err
		}
		urls := parseBody(res)
		fmt.Printf("Found %d urls.\n", len(urls))

		for _, u := range urls {
			fmt.Printf("Inserting url: %s\n", u)
			err := ddbinsert(sess, u, item.Count)
			if err != nil {
				return "", err
			}
		}
	}

	return fmt.Sprintln(event), nil
}

func main() {
	lambda.Start(HandleRequest)
}

func ddbinsert(sess *session.Session, u string, depth int) error {

	svc := dynamodb.New(sess)

	tableName := "crawler-urls"

	item := Item{
		URL:   u,
		Count: depth,
	}
	av, err := dynamodbattribute.MarshalMap(item)
	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(tableName),
	}
	_, err = svc.PutItem(input)
	if err != nil {
		return err
	}
	return nil
}

func geturl(url string) (res string, e error) {

	res, e = "", nil

	// client timeout for request
	timeout := 5 * time.Second
	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout: timeout,
		}).DialContext,
	}

	// Get the data
	client := &http.Client{
		Transport:     transport,
		CheckRedirect: redirect,
	}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		e = err
		return
	}

	resp, err := client.Do(req)

	if err != nil {
		e = err
		return
	}

	buf := new(bytes.Buffer)
	buf.ReadFrom(resp.Body)
	res = buf.String()

	defer resp.Body.Close()
	return
}

func redirect(req *http.Request, via []*http.Request) error {
	return http.ErrUseLastResponse
}

func parseBody(body string) (res []string) {
	res = []string{}
	re := regexp.MustCompile(`(http|https)://([\w_-]+(?:(?:\.[\w_-]+)+))([\w.,@?^=%&:/~+#-]*[\w@?^=%&/~+#-])?`)
	matches := re.FindAll([]byte(body), -1)
	for _, m := range matches {
		res = append(res, string(m))
	}
	return
}
