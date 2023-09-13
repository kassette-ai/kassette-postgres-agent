package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"github.com/spf13/viper"
)

type TableRow map[string]interface{}

type Payload struct {
	Batch []map[string]interface{} `json:"batch"`
}

type TableConfig struct {
	Agent  string              `json:"kassette_data_agent"`
	Type   string              `json:"kassette_data_type"`
	Config []map[string]string `json:"config"`
}

type SourceAdvancedConfig struct {
	Source []TableConfig `json:"source_config"`
}

func GetConnectionString() string {
	return fmt.Sprintf("host=%s port=%s user=%s "+
		"password=%s dbname=%s sslmode=%s",
		viper.GetString("database.host"),
		viper.GetString("database.port"),
		viper.GetString("database.user"),
		viper.GetString("database.password"),
		viper.GetString("database.name"),
		viper.GetString("database.ssl_mode"))
}

func submitPayload(jsonData []byte, payloadType string) {
	var url string
	baseUrl := viper.GetString("kassette-server.url")
	uid := viper.GetString("kassette-agent.uid")
	maxAttempts := 20
	initialBackoff := 1 * time.Second
	maxBackoff := 10 * time.Second
	if payloadType == "batch" {
		url = baseUrl + "/extract"
	} else if payloadType == "configtable" {
		url = baseUrl + "/configtable"
	} else {
		log.Fatal("Unknown payload type: ", payloadType)
	}
	for attempt := 1; attempt <= maxAttempts; attempt++ {

		req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
		if err != nil {
			log.Fatal("Error creating request:", err)
			return
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("write_key", uid)
		// Send the request
		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			log.Printf("Error sending request: %s", err)
			backoff := time.Duration(attempt-1) * initialBackoff
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			time.Sleep(backoff)
			continue
		}
		defer resp.Body.Close()
		// Check the response status code
		if resp.StatusCode != http.StatusOK {
			log.Fatal("Request failed with status:", resp.StatusCode)
			return
		}
		log.Printf("Request successful!\n")
		return
	}
	log.Fatal("Max retry attempts reached")
}

func startWorker(activitiInstances []map[string]interface{}) {
	// create the payload
	var payload Payload
	payload.Batch = activitiInstances

	jsonData, err := json.Marshal(payload)
	if err != nil {
		log.Fatal(err)
		return
	}
	log.Printf("Json object: %s", string(jsonData))
	submitPayload(jsonData, "batch")
}

func get_new_records_by_id(dbHandler *sql.DB, tableName string, dbBatchSize string, idColumn string, trackPosition int64) (int64, []map[string]interface{}) {
	var lastTrackPosition int64
	var ok bool

	query := fmt.Sprintf("SELECT * FROM %s where %s > $1 order by %s;", tableName, idColumn, dbBatchSize)
	// Execute the SQL statement and retrieve the rows
	rows, err := dbHandler.QueryContext(context.Background(), query, trackPosition)
	if err != nil {
		log.Fatal(err)
	}
	data := make([]map[string]interface{}, 0)

	// Iterate over the rows
	for rows.Next() {
		// Create a map to hold the row data
		record := make(TableRow)

		// Get the column names
		columns, err := rows.Columns()
		if err != nil {
			log.Fatal(err)
		}

		// Create a slice to hold the column values
		values := make([]interface{}, len(columns))
		for i := range values {
			values[i] = new(interface{})
		}

		// Scan the row values into the slice
		err = rows.Scan(values...)
		if err != nil {
			log.Fatal(err)
		}

		// Convert the row values into a JSON object
		for i, column := range columns {
			val := *(values[i].(*interface{}))
			record[column] = val
		}
		//Adding Kassette Metadata
		record["kassette_data_agent"] = "camunda"
		record["kassette_data_type"] = tableName

		data = append(data, record)
		lastTrackPosition, ok = record[idColumn].(int64)
		if !ok {
			log.Fatal("Invalid value type int64")
		}
	}
	// Check for any errors during iteration
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}
	return lastTrackPosition, data
}

func get_new_records(dbHandler *sql.DB, tableName string, dbBatchSize string, trackColumn string, trackPosition time.Time, idColumn string, idExclude []string) (time.Time, []string, []map[string]interface{}) {
	var lastTrackPosition time.Time
	fetchedIds := make([]string, 0)
	query := fmt.Sprintf("SELECT * FROM %s where %s > $1 and %s not in ($2) limit %s;", tableName, trackColumn, idColumn, dbBatchSize)
	// Execute the SQL statement and retrieve the rows
	//log.Printf(fmt.Sprintf("%s, %s, %s", query, trackPosition, strings.Join(idExclude, ", ")))
	rows, err := dbHandler.QueryContext(context.Background(), query, trackPosition, strings.Join(idExclude, ", "))
	if err != nil {
		log.Fatal(err)
	}
	data := make([]map[string]interface{}, 0)

	// Iterate over the rows
	for rows.Next() {
		// Create a map to hold the row data
		record := make(TableRow)

		// Get the column names
		columns, err := rows.Columns()
		if err != nil {
			log.Fatal(err)
		}

		// Create a slice to hold the column values
		values := make([]interface{}, len(columns))
		for i := range values {
			values[i] = new(interface{})
		}

		// Scan the row values into the slice
		err = rows.Scan(values...)
		if err != nil {
			log.Fatal(err)
		}

		// Convert the row values into a JSON object
		for i, column := range columns {
			val := *(values[i].(*interface{}))
			record[column] = val
		}
		//Adding Kassette Metadata
		//record["kassette_data_agent"] = "camunda"
		//record["kassette_data_type"] = tableName

		fetchedId, ok := record[idColumn].(string)
		if !ok {
			log.Fatal("Invalid value type string")
		} else {
			fetchedIds = append(fetchedIds, fetchedId)
		}
		timestamp, ok := record[trackColumn].(time.Time)
		if !ok {
			log.Fatal("Invalid value type time.Time")
		} else {
			lastTrackPosition = timestamp
		}
		data = append(data, record)
	}
	// Check for any errors during iteration
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}
	return lastTrackPosition, fetchedIds, data
}
func get_table_schema(dbHandler *sql.DB, tableName string) []map[string]string {
	query := fmt.Sprintf("SELECT column_name,data_type FROM information_schema.columns WHERE table_name = '%s';", tableName)
	// Execute the SQL statement and retrieve the rows
	rows, err := dbHandler.QueryContext(context.Background(), query)
	if err != nil {
		log.Fatal(err)
	}
	var keyValuePairs []map[string]string

	for rows.Next() {
		var key, value string
		err := rows.Scan(&key, &value)
		if err != nil {
			log.Fatal(err)
		}

		pair := map[string]string{
			key: value,
		}

		keyValuePairs = append(keyValuePairs, pair)
	}

	return keyValuePairs

}

func main() {
	// Load Config file
	viper.SetConfigFile("config.yaml")
	viper.SetConfigType("yaml")
	// Load configuration from environment variables
	viper.AutomaticEnv()

	verr := viper.ReadInConfig()
	if verr != nil {
		log.Println(verr)
		return
	}

	psqlInfo := GetConnectionString()
	lastTimeStamp := time.Now().Add(-2 * time.Hour) //start ingesting data 2 hours back after restart
	var lastStamp int64 = 0
	batchSubmit := make([]map[string]interface{}, 0)
	kassetteBatchSize := viper.GetInt("kassette-server.batchSize")
	//read tables settings into Map
	var trackTables map[string]map[string]string
	var trackTablesTs map[string]map[string]interface{}
	trackTablesTs = make(map[string]map[string]interface{})
	trackTables = make(map[string]map[string]string)
	for table, _ := range viper.GetStringMapString("tables") {
		trackTablesTs[table] = make(map[string]interface{})
		trackTables[table] = make(map[string]string)
		for tableSettingKey, tableSettingValue := range viper.GetStringMapString("tables." + table) {
			trackTables[table][tableSettingKey] = tableSettingValue
		}

		if trackTables[table]["track_column"] == trackTables[table]["id_column"] {
			trackTables[table]["tracking"] = "id"
			trackTablesTs[table]["lastId"] = lastStamp
		} else {
			trackTables[table]["tracking"] = "time"
			trackTablesTs[table]["lastTimeStamp"] = lastTimeStamp
			trackTablesTs[table]["lastFetched"] = make([]string, 0)
		}

	}

	dbBatchSize := viper.GetString("database.batchSize")
	log.Printf("Connecting to Database: %s\n", psqlInfo)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create a ticker that polls the database every 10 seconds
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			for table, tableData := range trackTables {
				if tableData["tracking"] == "time" {
					lastFetched, ok := trackTablesTs[table]["lastFetched"].([]string)
					if !ok {
						log.Fatal("Type Error Array of strings")
					}
					lastLastTimeStamp, lastLastFetched, batch := get_new_records(db, table, dbBatchSize, tableData["track_column"], lastTimeStamp, tableData["id_column"], lastFetched)
					// Update the last seen timestamp of processed record
					// or store IDs of records belonging to the same timestamp to exclude them from the next select
					// to avoid duplication
					ts, ok := trackTablesTs[table]["lastTimeStamp"].(time.Time)
					if !ok {
						log.Fatal(fmt.Printf("Type Error: %s", ts))
					}
					if lastLastTimeStamp.After(ts) {
						trackTablesTs[table]["lastTimeStamp"] = lastLastTimeStamp
						trackTablesTs[table]["lastFetched"] = lastFetched[:0]
					} else {
						trackTablesTs[table]["lastFetched"] = append(lastFetched, lastLastFetched...)
					}
					batchSubmit = append(batchSubmit, batch...)
					if len(batchSubmit) >= kassetteBatchSize {
						startWorker(batchSubmit) //submit a batch if number of records enough
						batchSubmit = nil
					}
				} else if tableData["tracking"] == "id" {
					lastId, ok := trackTablesTs[table]["lastId"].(int64)
					if !ok {
						log.Fatal(fmt.Printf("Type Error: %s", lastId))
					}
					lastStamp, batch := get_new_records_by_id(db, table, dbBatchSize, tableData["id_column"], lastId)
					batchSubmit = append(batchSubmit, batch...)
					if len(batchSubmit) >= kassetteBatchSize {
						startWorker(batchSubmit) //submit a batch if number of records enough
						batchSubmit = nil
					}
					trackTablesTs[table]["lastId"] = lastStamp
				}

			}
			if len(batchSubmit) > 0 { //submit a batch if anything left after a cycle
				startWorker(batchSubmit)
				batchSubmit = nil
			}
		}
	}
}
