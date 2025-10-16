package api

import (
	"net/http"

	"github.com/alexdev-tb/CodePortal/internal/auth"
)

type Router struct {
	mux *http.ServeMux
}

func NewRouter(handler *Handler, authHandler *auth.Handler) http.Handler {
	r := &Router{mux: http.NewServeMux()}
	r.registerRoutes(handler, authHandler)
	return corsMiddleware(r.mux)
}

func (r *Router) registerRoutes(handler *Handler, authHandler *auth.Handler) {
	// Health endpoint
	r.mux.HandleFunc("/health", handler.Health)
	
	// Execution endpoints
	r.mux.HandleFunc("/v1/execute", handler.Execute)
	r.mux.HandleFunc("/v1/execute/", handler.GetJob)
	
	// Auth endpoints
	r.mux.HandleFunc("/v1/auth/register", authHandler.Register)
	r.mux.HandleFunc("/v1/auth/login", authHandler.Login)
	r.mux.HandleFunc("/v1/auth/validate", authHandler.ValidateToken)
}

// corsMiddleware adds CORS headers to all responses
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Set CORS headers
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		w.Header().Set("Access-Control-Max-Age", "86400")

		// Handle preflight OPTIONS request
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}
