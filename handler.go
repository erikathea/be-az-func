package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"github.com/erikathea/migp-go/pkg/migp"
	_ "github.com/lib/pq"
)

// kvStore is a wrapper for a KV store backed by PostgreSQL.
type kvStore struct {
	db *sql.DB
}

// newKVStore initializes a new kvStore with a PostgreSQL database connection.
func newKVStore(db *sql.DB) (*kvStore, error) {
	kv := &kvStore{db: db}

	query := `
	CREATE TABLE IF NOT EXISTS kv_store (
		id TEXT NOT NULL,
		value BYTEA,
		PRIMARY KEY (id)
	) PARTITION BY HASH (id);

	CREATE TABLE IF NOT EXISTS kv_store_p0 PARTITION OF kv_store FOR VALUES WITH (MODULUS 4, REMAINDER 0);
	CREATE TABLE IF NOT EXISTS kv_store_p1 PARTITION OF kv_store FOR VALUES WITH (MODULUS 4, REMAINDER 1);
	CREATE TABLE IF NOT EXISTS kv_store_p2 PARTITION OF kv_store FOR VALUES WITH (MODULUS 4, REMAINDER 2);
	CREATE TABLE IF NOT EXISTS kv_store_p3 PARTITION OF kv_store FOR VALUES WITH (MODULUS 4, REMAINDER 3);

	CREATE TABLE IF NOT EXISTS kv_store_shadow (
		id TEXT,
		value BYTEA,
		PRIMARY KEY (id, value)
	);
	CREATE INDEX IF NOT EXISTS kv_store_shadow_values ON kv_store_shadow (value);
	`
	_, err := db.Exec(query)
	if err != nil {
		return nil, err
	}

	return kv, nil
}

// Get returns the value in the key identified by id.
func (kv *kvStore) Get(id string) ([]byte, error) {
	query := `SELECT value FROM kv_store WHERE id = $1`
	var value []byte
	err := kv.db.QueryRow(query, id).Scan(&value)
	if err != nil {
		if err == sql.ErrNoRows {
			return []byte{}, nil
		}
		return nil, err
	}
	return value, nil
}

// newServer returns a new server initialized using the provided configuration
func newServer(cfg migp.ServerConfig) (*server, error) {
	migpServer, err := migp.NewServer(cfg)
	if err != nil {
		return nil, err
	}

	dbConnectionString := os.Getenv("DB_CONNECTION_ST")
	if dbConnectionString == "" {
		log.Println("DB_CONNECTION_ST environment variable not set. Using default localhost connection string.")
		dbConnectionString = "user=user password=pw dbname=db sslmode=disable host=localhost"
	}

	log.Printf("Using database connection string: %s", dbConnectionString)
	db, err := sql.Open("postgres", dbConnectionString)
	if err != nil {
		log.Fatalf("Error opening database: %v", err)
	}

	if err = db.Ping(); err != nil {
		log.Fatalf("Error connecting to the database: %v", err)
	}

	kv, err := newKVStore(db)
	if err != nil {
		return nil, err
	}

	return &server{
		migpServer: migpServer,
		kv:         kv,
	}, nil
}

// server wraps a MIGP server and backing KV store
type server struct {
	migpServer *migp.Server
	kv         *kvStore
}

// handler handles client requests
func (s *server) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/query", s.handleEvaluate)
	return mux
}

// handleIndex returns a welcome message
func (s *server) handleIndex(w http.ResponseWriter, req *http.Request) {
	fmt.Fprintf(w, "Welcome to the MIGP demo server\n")
}

// handleConfig returns the MIGP configuration
func (s *server) handleConfig(w http.ResponseWriter, req *http.Request) {
	encoder := json.NewEncoder(w)
	cfg := s.migpServer.Config().Config
	if err := encoder.Encode(cfg); err != nil {
		log.Println("Writing response failed:", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
	}
}

// handleEvaluate serves a request from a MIGP client
func (s *server) handleEvaluate(w http.ResponseWriter, req *http.Request) {
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Println("Request body reading failed:", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	var request migp.ClientRequest
	if err := json.Unmarshal(body, &request); err != nil {
		log.Println("Request body unmarshal failed:", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
	}

	migpResponse, err := s.migpServer.HandleRequest(request, s.kv)
	if err != nil {
		log.Println("HandleRequest failed:", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")

	respBody, err := migpResponse.MarshalBinary()
	log.Println(respBody)
	if err != nil {
		log.Println("Response serialization failed:", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
	}
	if _, err := w.Write(respBody); err != nil {
		log.Println("Writing response failed:", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
	}
}

func main() {
	listenAddr := ":8080"
	if val, ok := os.LookupEnv("FUNCTIONS_CUSTOMHANDLER_PORT"); ok {
		listenAddr = ":" + val
	}

	var config migp.ServerConfig
	configJSON := os.Getenv("CONFIG_JSON")
	if configJSON == "" {
		log.Fatal("CONFIG_JSON environment variable not set")
	}

	err := json.Unmarshal([]byte(configJSON), &config)
	if err != nil {
		log.Fatalf("Error parsing CONFIG_JSON: %v", err)
	}
	s, err := newServer(config)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("About to listen on %s", listenAddr)
	log.Fatal(http.ListenAndServe(listenAddr, s.handler()))
}
