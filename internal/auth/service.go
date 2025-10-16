package auth

import (
	"errors"
	"strings"
)

// Service provides authentication functionality
type Service struct {
	store      Store
	jwtService *JWTService
}

func NewService(store Store, jwtSecret string) *Service {
	return &Service{
		store:      store,
		jwtService: NewJWTService(jwtSecret),
	}
}

// Register creates a new user account
func (s *Service) Register(req RegisterRequest) (*AuthResponse, error) {
	// Validate input
	if err := s.validateRegisterRequest(req); err != nil {
		return nil, err
	}

	// Create user
	user, err := s.store.CreateUser(req)
	if err != nil {
		return nil, err
	}

	// Generate token
	token, err := s.jwtService.GenerateToken(*user)
	if err != nil {
		return nil, err
	}

	return &AuthResponse{
		User:  *user,
		Token: token,
	}, nil
}

// Login authenticates a user and returns a token
func (s *Service) Login(req LoginRequest) (*AuthResponse, error) {
	// Validate input
	if err := s.validateLoginRequest(req); err != nil {
		return nil, err
	}

	// Validate credentials
	user, err := s.store.ValidateCredentials(req.Email, req.Password)
	if err != nil {
		return nil, err
	}

	// Generate token
	token, err := s.jwtService.GenerateToken(*user)
	if err != nil {
		return nil, err
	}

	return &AuthResponse{
		User:  *user,
		Token: token,
	}, nil
}

// ValidateToken validates a JWT token and returns the user
func (s *Service) ValidateToken(tokenString string) (*User, error) {
	claims, err := s.jwtService.ValidateToken(tokenString)
	if err != nil {
		return nil, err
	}

	user, err := s.store.GetUserByID(claims.UserID)
	if err != nil {
		return nil, err
	}

	return user, nil
}

// ExtractTokenFromHeader extracts JWT token from Authorization header
func (s *Service) ExtractTokenFromHeader(authHeader string) (string, error) {
	return s.jwtService.ExtractTokenFromHeader(authHeader)
}

func (s *Service) validateRegisterRequest(req RegisterRequest) error {
	if strings.TrimSpace(req.Name) == "" {
		return errors.New("name is required")
	}
	if strings.TrimSpace(req.Email) == "" {
		return errors.New("email is required")
	}
	if !isValidEmail(req.Email) {
		return errors.New("invalid email format")
	}
	if len(req.Password) < 12 {
		return errors.New("password must be at least 12 characters long")
	}
	return nil
}

func (s *Service) validateLoginRequest(req LoginRequest) error {
	if strings.TrimSpace(req.Email) == "" {
		return errors.New("email is required")
	}
	if strings.TrimSpace(req.Password) == "" {
		return errors.New("password is required")
	}
	return nil
}

func isValidEmail(email string) bool {
	// Simple email validation
	return strings.Contains(email, "@") && strings.Contains(email, ".")
}