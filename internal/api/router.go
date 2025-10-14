package api

import (
	"net/http"
)

type Router struct {
	mux *http.ServeMux
}

func NewRouter(handler *Handler) http.Handler {
	r := &Router{mux: http.NewServeMux()}
	r.registerRoutes(handler)
	return r.mux
}

func (r *Router) registerRoutes(handler *Handler) {
	r.mux.HandleFunc("/health", handler.Health)
	r.mux.HandleFunc("/v1/execute", handler.Execute)
	r.mux.HandleFunc("/v1/execute/", handler.GetJob)
}
