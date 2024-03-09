package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"log"
	"os"
	"time"
)

type Myresponse struct {
	Message int32
}

func main() {
	godotenv.Load()
	lambda.Start(requestHandler)
}

func requestHandler(ctx context.Context, sqsEvent events.SQSEvent) (Myresponse , error) {

	connStr := os.Getenv("DB_URL")
	db, err := sql.Open("postgres", connStr)

	
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	if err = db.Ping(); err != nil {
		log.Fatal(err)
	}

	query := `SELECT * FROM userdetails WHERE apiKey = $1`

	for _ , mes  := range sqsEvent.Records {

		var name string
		var email string
		var apikey string
		var hits int32
		var plantype string
		var expiryon string

		err := db.QueryRow(query, mes.Body).Scan(&name, &email, &plantype, &apikey, &expiryon, &hits)
		if err != nil { log.Fatal("QUERY RETURNED: ", err) }

		rateLimit(apikey, plantype, hits, expiryon, db)
	}

	return Myresponse{200}, nil
}

func DateToString(year int, month time.Month, day int) string {
	return fmt.Sprintf("%d-%02d-%02d", year, int(month), day)
}  

func rateLimit(apiKey string, plantype string, hits int32, expiryon string, db *sql.DB) {

	year, month, day := time.Now().Date()
	currentDate := DateToString(year, month, day)

	switch plantype {

	case "Hobby":
		{
			if hits > 100 {
				db.Exec("UPDATE userkeystatus SET status = 'Daily limit reached' WHERE apikey = $1;", apiKey)
				println("A user with Hobby plan reached its daily limit")
			} else {
				db.Exec("UPDATE userdetails SET hits = hits + 1 WHERE apikey = $1;", apiKey)
				println("A user with hobby plan incremented its hits !!")
			}
		}

	case "Priority":
		{
			println("DATES: ", expiryon[:10], currentDate)

			// expirion is this format: 2023-03-09T00:00:00Z hence slicing it till 10th charcater ..

			if expiryon[:10] == currentDate {
				db.Exec("UPDATE userdetails SET plantype = 'Hobby', hits = 1 WHERE apikey = $1", apiKey)
				println("User's priority plan with has expired !")
				// send some notification to the user that his priority plan has expired and he has been re-subscribed to hobby plan
			}

			if hits > 5000 {
				db.Exec("UPDATE userkeystatus SET status = 'Daily limit reached' WHERE apikey = $1;", apiKey)
				println("A user with Priority plan reached its daily limit")
			} else {
				db.Exec("UPDATE userdetails SET hits = hits + 1 WHERE apikey = $1;", apiKey)
				println("A user with Priority plan incremented its hits !!")
			}
		}

	case "Enterprize":
		{
			println("ENTERPRIZE PLAN EXPIRED RE-SUBSCRIBE TO YOUR ENTERPRIZE PLAN TO CONTINUE !!")
		}

	}
}
