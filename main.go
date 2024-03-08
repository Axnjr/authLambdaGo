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

	defer db.Close()

	if err != nil {
		log.Fatal(err)
	}
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

func rateLimit(apiKey string, plantype string, hits int32, expiryon string, db *sql.DB) {
	year, month, day := time.Now().Date()
	currentDate := fmt.Sprint("%y-%m-%a", year, month, day)

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
			println("DATES: ", expiryon, currentDate)

			if expiryon == "2024-03-03" {
				db.Exec("UPDATE userdetails SET plantype = 'Hobby', hits = 1 WHERE apikey = $1")
				println("User's priority plan with has expired !")
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
