package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/rs/cors"
)

// Database connection pool
var db *pgxpool.Pool

// SQLQuery represents the structure of a query request
type SQLQuery struct {
	Query string `json:"query"`
}

func main() {
	var err error
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		log.Fatal("DATABASE_URL environment variable is required")
	}

	db, err = pgxpool.Connect(context.Background(), dbURL)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v\n", err)
	}
	defer db.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/query", queryHandler)

	handler := cors.Default().Handler(mux)
	log.Println("Starting server on :8080...")
	log.Fatal(http.ListenAndServe(":8080", handler))
}

func queryHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	var sqlQuery SQLQuery
	err := json.NewDecoder(r.Body).Decode(&sqlQuery)
	if err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	rows, err := db.Query(context.Background(), sqlQuery.Query)
	if err != nil {
		http.Error(w, fmt.Sprintf("Query error: %v", err), http.StatusBadRequest)
		return
	}
	defer rows.Close()

	// Retrieve column names
	fieldDescriptions := rows.FieldDescriptions()
	columns := getColumnNames(fieldDescriptions)

	// Prepare the response writer for gzip compression
	w.Header().Set("Content-Encoding", "gzip")
	w.Header().Set("Content-Type", "application/json")
	gz := gzip.NewWriter(w)
	defer gz.Close()

	// Stream the JSON response
	encoder := json.NewEncoder(gz)

	// Write column names first
	queryResponse := map[string]interface{}{
		"columns": columns,
		"rows":    [][]interface{}{},
	}

	if err := encoder.Encode(queryResponse); err != nil {
		http.Error(w, fmt.Sprintf("Error encoding response: %v", err), http.StatusInternalServerError)
		return
	}

	// Stream rows
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			http.Error(w, fmt.Sprintf("Error reading row: %v", err), http.StatusInternalServerError)
			return
		}

		// Encode each row individually
		row := map[string]interface{}{
			"rows": [][]interface{}{values},
		}
		if err := encoder.Encode(row); err != nil {
			http.Error(w, fmt.Sprintf("Error encoding row: %v", err), http.StatusInternalServerError)
			return
		}
	}

	if rows.Err() != nil {
		http.Error(w, fmt.Sprintf("Query error: %v", rows.Err()), http.StatusInternalServerError)
		return
	}
}

func getColumnNames(columns []pgproto3.FieldDescription) []string {
	names := make([]string, len(columns))
	for i, col := range columns {
		names[i] = string(col.Name)
	}
	return names
}
