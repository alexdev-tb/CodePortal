package auth

import (
	"database/sql"
	"errors"
	"time"

	_ "github.com/lib/pq"
	"golang.org/x/crypto/bcrypt"
)

// PostgresStore implements Store interface using PostgreSQL
type PostgresStore struct {
	db *sql.DB
}

func NewPostgresStore(db *sql.DB) (*PostgresStore, error) {
	store := &PostgresStore{db: db}
	if err := store.createTables(); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *PostgresStore) createTables() error {
	query := `
	CREATE TABLE IF NOT EXISTS users (
		id VARCHAR(32) PRIMARY KEY,
		name VARCHAR(255) NOT NULL,
		email VARCHAR(255) UNIQUE NOT NULL,
		organization VARCHAR(255),
		password_hash VARCHAR(255) NOT NULL,
		created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
		updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
	);

	CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
	`

	_, err := s.db.Exec(query)
	return err
}

func (s *PostgresStore) CreateUser(req RegisterRequest) (*User, error) {
	// Check if user already exists
	var exists bool
	err := s.db.QueryRow("SELECT EXISTS(SELECT 1 FROM users WHERE LOWER(email) = LOWER($1))", req.Email).Scan(&exists)
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, ErrUserAlreadyExists
	}

	// Hash password
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		return nil, err
	}

	// Generate user ID
	id, err := generateID()
	if err != nil {
		return nil, err
	}

	// Create user
	now := time.Now()
	query := `
		INSERT INTO users (id, name, email, organization, password_hash, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING id, name, email, organization, created_at, updated_at
	`

	user := &User{}
	err = s.db.QueryRow(query, id, req.Name, req.Email, req.Organization, string(hashedPassword), now, now).Scan(
		&user.ID, &user.Name, &user.Email, &user.Organization, &user.CreatedAt, &user.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	return user, nil
}

func (s *PostgresStore) GetUserByEmail(email string) (*User, error) {
	query := `
		SELECT id, name, email, organization, password_hash, created_at, updated_at
		FROM users 
		WHERE LOWER(email) = LOWER($1)
	`

	user := &User{}
	err := s.db.QueryRow(query, email).Scan(
		&user.ID, &user.Name, &user.Email, &user.Organization, &user.PasswordHash, &user.CreatedAt, &user.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrUserNotFound
		}
		return nil, err
	}

	return user, nil
}

func (s *PostgresStore) GetUserByID(id string) (*User, error) {
	query := `
		SELECT id, name, email, organization, password_hash, created_at, updated_at
		FROM users 
		WHERE id = $1
	`

	user := &User{}
	err := s.db.QueryRow(query, id).Scan(
		&user.ID, &user.Name, &user.Email, &user.Organization, &user.PasswordHash, &user.CreatedAt, &user.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrUserNotFound
		}
		return nil, err
	}

	return user, nil
}

func (s *PostgresStore) ValidateCredentials(email, password string) (*User, error) {
	user, err := s.GetUserByEmail(email)
	if err != nil {
		return nil, ErrInvalidCredentials
	}

	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(password)); err != nil {
		return nil, ErrInvalidCredentials
	}

	return user, nil
}