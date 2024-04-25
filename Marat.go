package assignment_3

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"net/http"
	"time"
)

const (
	username = "marat"
	password = "password"
	hostname = "127.0.0.1"
	port     = 5432
	database = "postgres"
	schema   = "test"
)

var redisClient *redis.Client
var db *sql.DB

type Club struct {
	ID     int    `json:"id"`
	Name   string `json:"name"`
	City   string `json:"city"`
	League string `json:"league"`
}

func connectRedis() error {
	redisClient = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	_, err := redisClient.Ping(context.Background()).Result()
	return err
}

func connectDatabase() error {
	DSN := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?search_path=%s", username, password, hostname, port, database, schema)
	var err error
	db, err = sql.Open("postgres", DSN)
	if err != nil {
		return err
	}
	err = db.Ping()
	return err
}

func getClubByIdFromDb(id string) (*Club, error) {
	query := "SELECT * FROM clubs WHERE id = " + id
	fmt.Println(query)
	row := db.QueryRow(query)
	club := &Club{}
	err := row.Scan(&club.ID, &club.Name, &club.City, &club.League)
	if err != nil {
		fmt.Println("error")
		return nil, err
	}
	return club, nil
}

func dbInsertClub(club *Club) error {
	fmt.Println(club.Name, club)
	_, err := db.Exec("INSERT INTO clubs(name, city, league) VALUES ($1, $2, $3)", club.Name, club.City, club.League)
	return err
}

func addClub(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	err1 := r.ParseForm()
	if err1 != nil {
		http.Error(w, "Error parsing form", http.StatusBadRequest)
		return
	}
	club := &Club{
		Name:   r.Form.Get("name"),
		City:   r.Form.Get("city"),
		League: r.Form.Get("league"),
	}
	err := dbInsertClub(club)
	if err != nil {
		_, err2 := fmt.Fprintf(w, "Error!")
		if err2 != nil {
			return
		}
	}
}

func marshalClub(club *Club) ([]byte, error) {
	return json.Marshal(club)
}

func getClub(r *http.Request) (*Club, error) {
	err := r.ParseForm()
	if err != nil {
		return nil, err
	}
	clubID := r.Form.Get("id")
	cacheKey := fmt.Sprintf("club:%s", clubID)

	cacheResult, err := redisClient.Get(context.Background(), cacheKey).Bytes()
	if err != nil {
		club, err := getClubByIdFromDb(clubID)
		fmt.Println("in MEMORY")
		if err != nil {
			fmt.Println("1")
			return nil, err
		}

		serializedClub, err := marshalClub(club)
		if err != nil {
			fmt.Println("Error marshaling club:", err)
			return nil, err
		}

		err = redisClient.Set(context.Background(), cacheKey, serializedClub, 5*time.Minute).Err() // Set expiry to 5 minutes
		if err != nil {
			fmt.Println("2")
			return nil, err
		}
		return club, nil
	} else {
		var club Club
		err := json.Unmarshal(cacheResult, &club)
		if err != nil {
			return nil, err
		}
		fmt.Println("in CACHE", club)
		return &club, nil
	}
}

func getClubHandler(w http.ResponseWriter, r *http.Request) {
	club, err := getClub(r)
	if err != nil {
		http.Error(w, "Error getting club: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if club == nil {
		http.Error(w, "Club not found", http.StatusNotFound)
		return
	}

	jsonData, err := json.Marshal(club)
	if err != nil {
		http.Error(w, "Error marshalling club to JSON: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK) // Explicitly set the status code to 200 OK
	w.Write(jsonData)
}

func main() {
	err := connectRedis()
	if err != nil {
		fmt.Print(err)
		return
	}
	err = connectDatabase()
	if err != nil {
		fmt.Print(err)
		return
	}
	http.HandleFunc("/add-club", addClub)
	http.HandleFunc("/get-club", getClubHandler)

	fmt.Println("Server is running on port 8080...")
	http.ListenAndServe(":8080", nil)
}
