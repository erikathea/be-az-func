package main

import (
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"os"

	"github.com/erikathea/migp-go/pkg/migp"
	"github.com/erikathea/migp-go/pkg/mutator"
	_ "github.com/lib/pq"
)

// kvStore is a wrapper for a KV store backed by PostgreSQL.
type kvStore struct {
	db *sql.DB
}

// newKVStore initializes a new kvStore with a PostgreSQL database connection.
func newKVStore(db *sql.DB) (*kvStore, error) {
	kv := &kvStore{db: db}

	// Create the table if it doesn't exist
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
	);`
	_, err := db.Exec(query)
	if err != nil {
		return nil, err
	}

	return kv, nil
}

// Put a value at key id and replace any existing value.
func (kv *kvStore) Put(id string, value []byte) error {
	query := `
	INSERT INTO kv_store (id, value) VALUES ($1, $2)
	ON CONFLICT (id) DO UPDATE SET value = $2;`
	_, err := kv.db.Exec(query, id, value)
	return err
}

// Put a value at key id and replace any existing value.
func (kv *kvStore) insertShadow(id string, value []byte) error {
	query := `
	INSERT INTO kv_store_shadow (id, value) VALUES ($1, $2)
	ON CONFLICT (id, value) DO NOTHING;`
	_, err := kv.db.Exec(query, id, value)
	return err
}

// Append a value to any existing value at key id.
func (kv *kvStore) Append(id string, value []byte) error {
	query := `SELECT value FROM kv_store WHERE id = $1`
	var existingValue []byte
	err := kv.db.QueryRow(query, id).Scan(&existingValue)
	if err != nil {
		if err == sql.ErrNoRows {
			return kv.Put(id, value)
		}
		return err
	}

	newValue := append(existingValue, value...)
	return kv.Put(id, newValue)
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

// checkIfUnique checks if the value for a given id is unique in the shadow table.
func (kv *kvStore) checkIfUnique(id string, value []byte) bool {
	query := `SELECT 1 FROM kv_store_shadow WHERE id = $1 AND value = $2`
	var exists int
	err := kv.db.QueryRow(query, id, value).Scan(&exists)
	return err == sql.ErrNoRows
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
		dbConnectionString = "user=cs-db password=hacker dbname=cs-db sslmode=disable host=localhost"
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
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/evaluate", s.handleEvaluate)
	mux.HandleFunc("/config", s.handleConfig)
	return mux
}

// GenerateRandomString generates a random λ-bits long string
func GenerateRandomString(bits int) ([]byte, error) {
	bytes := int(math.Ceil(float64(bits) / 8.0))
	randomBytes := make([]byte, bytes)

	_, err := rand.Read(randomBytes)
	if err != nil {
		return nil, err
	}

	return randomBytes, nil
}

// insert encrypts a credential pair and stores it in the configured KV store
func (s *server) insert(username, password, metadata []byte, numVariants int, includeUsernameVariant, phaseOne bool) error {
	var (
		newEntry []byte
		err      error
	)
	bucketIDHex := migp.BucketIDToHex(s.migpServer.BucketID(username))
	log.Println("----ID ", bucketIDHex)
	if phaseOne {
		newEntry, err := s.migpServer.EncryptBucketEntry(username, password, migp.MetadataBreachedPassword, metadata)
		if err != nil {
			return err
		}

		err = s.kv.Append(bucketIDHex, newEntry)
		if err != nil {
			return err
		}
		s.kv.insertShadow(bucketIDHex, newEntry)
		log.Println("newEntry ", base64.StdEncoding.EncodeToString(newEntry))

		if includeUsernameVariant {
			newEntry, err = s.migpServer.EncryptBucketEntry(username, nil, migp.MetadataBreachedUsername, metadata)
			if err != nil {
				return err
			}

			err = s.kv.Append(bucketIDHex, newEntry)
			if err != nil {
				return err
			}
			s.kv.insertShadow(bucketIDHex, newEntry)
			log.Println("-- includeUsernameVariant ", base64.StdEncoding.EncodeToString(newEntry))
		}
		return nil
	} else {
		passwordVariants := mutator.NewRDasMutator().Mutate(password, numVariants)
		for _, variant := range passwordVariants {
			newEntry, err = s.migpServer.EncryptBucketEntry(username, variant, migp.MetadataSimilarPassword, metadata)
			// Ensure the value is unique before appending
			for !s.kv.checkIfUnique(bucketIDHex, newEntry) {
				randomString, _ := GenerateRandomString(256)
				altVariant := mutator.NewRDasMutator().Mutate(randomString, 1)
				log.Println(".    altVariant - ", string(altVariant[0]))
				newEntry, err = s.migpServer.EncryptBucketEntry(username, altVariant[0], migp.MetadataSimilarPassword, metadata)
			}
			if err != nil {
				return err
			}
			err = s.kv.Append(bucketIDHex, newEntry)
			if err != nil {
				return err
			}
			s.kv.insertShadow(bucketIDHex, newEntry)
			log.Println("-- password variant ", base64.StdEncoding.EncodeToString(newEntry))

		}
	}

	bucketContents, err := s.kv.Get(bucketIDHex)
	log.Println("content ", base64.StdEncoding.EncodeToString(bucketContents))
	log.Println("ID ", bucketIDHex)

	return nil
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
