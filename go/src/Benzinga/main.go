package main

import (
	"encoding/json"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
)

// Payload struct represents the expected JSON payload structure
type Payload struct {
	UserID int     `json:"user_id"`
	Total  float64 `json:"total"`
	Title  string  `json:"title"`
	Meta   Meta    `json:"meta"`
}

// Meta struct represents the 'meta'
type Meta struct {
	Logins       []Login      `json:"logins"`
	PhoneNumbers PhoneNumbers `json:"phone_numbers"`
	Completed    bool         `json:"completed"`
}

// Login struct represents the 'logins'
type Login struct {
	Time string `json:"time"`
	IP   string `json:"ip"`
}

// PhoneNumbers struct represents the 'phone_numbers'
type PhoneNumbers struct {
	Home   string `json:"home"`
	Mobile string `json:"mobile"`
}

var (
	batchSize     int
	batchInterval time.Duration
	postEndpoint  string
	logger        *logrus.Logger
	payloadCache  []Payload
)

func init() {
	logger = logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})

	batchSize = getEnvAsInt("BATCH_SIZE", 10)
	batchInterval = time.Duration(getEnvAsInt("BATCH_INTERVAL", 60)) * time.Second
	postEndpoint = getEnv("POST_ENDPOINT", "http://requestbin.net")

	logger.Info("Webhook receiver initialized")
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startTime := time.Now()
		next.ServeHTTP(w, r)
		duration := time.Since(startTime)

		logger.WithFields(logrus.Fields{
			"method":   r.Method,
			"endpoint": r.URL.Path,
			"duration": duration.String(),
		}).Info("Request processed")
	})
}

func main() {
	r := mux.NewRouter()

	// Middleware
	r.Use(loggingMiddleware)

	// Routes
	r.HandleFunc("/healthz", healthzHandler).Methods("GET")
	r.HandleFunc("/log", logHandler).Methods("POST")

	http.Handle("/", r)
	http.ListenAndServe(":8080", nil)
}

func healthzHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func logHandler(w http.ResponseWriter, r *http.Request) {
	var payload Payload
	err := json.NewDecoder(r.Body).Decode(&payload)
	if err != nil {
		logger.WithError(err).Error("Failed to decode JSON payload")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	payloadCache = append(payloadCache, payload)

	if len(payloadCache) >= batchSize {
		sendBatch()
	} else {
		go func() {
			time.Sleep(batchInterval)
			sendBatch()
		}()
	}

	w.WriteHeader(http.StatusOK)
}

func sendBatch() {
	logger.WithField("batch_size", len(payloadCache)).Info("Sending batch to external endpoint")

	startTime := time.Now()
	resp, err := http.Post(postEndpoint+"/batch", "application/json", nil)
	if err != nil {
		logger.WithError(err).Error("Failed to send batch to external endpoint")
		retryBatch()
		return
	}
	defer resp.Body.Close()

	duration := time.Since(startTime)
	logger.WithFields(logrus.Fields{
		"status_code": resp.StatusCode,
		"duration":    duration.String(),
	}).Info("Batch sent successfully")

	payloadCache = nil
}

func retryBatch() {
	for i := 0; i < 3; i++ {
		time.Sleep(2 * time.Second)
		logger.WithField("retry_attempt", i+1).Info("Retrying batch sending")
		sendBatch()
	}
	logger.Error("Failed to send batch after 3 retries. Exiting...")
	os.Exit(1)
}

func getEnv(key, defaultValue string) string {
	value, exists := os.LookupEnv(key)
	if !exists {
		return defaultValue
	}
	return value
}

func getEnvAsInt(key string, defaultValue int) int {
	value, exists := os.LookupEnv(key)
	if !exists {
		return defaultValue
	}
	intValue, err := strconv.Atoi(value)
	if err != nil {
		return defaultValue
	}
	return intValue
}
