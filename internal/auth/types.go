package auth

import (
	"time"
)

// User represents a user in the system
type User struct {
	ID           string    `json:"id"`
	Name         string    `json:"name"`
	Email        string    `json:"email"`
	Organization string    `json:"organization,omitempty"`
	PasswordHash string    `json:"-"` // Never include in JSON responses
	CreatedAt    time.Time `json:"createdAt"`
	UpdatedAt    time.Time `json:"updatedAt"`
}

// LoginRequest represents a login request payload
type LoginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

// RegisterRequest represents a registration request payload
type RegisterRequest struct {
	Name         string `json:"name"`
	Email        string `json:"email"`
	Organization string `json:"organization,omitempty"`
	Password     string `json:"password"`
}

// AuthResponse represents the response from login/register operations
type AuthResponse struct {
	User  User   `json:"user"`
	Token string `json:"token"`
}

// TokenClaims represents the JWT token claims
type TokenClaims struct {
	UserID string `json:"user_id"`
	Email  string `json:"email"`
	Exp    int64  `json:"exp"`
}