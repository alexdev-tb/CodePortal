package server

import (
	"context"
	"errors"
	"net/http"
	"strconv"

	"github.com/alexdev-tb/code-sandbox-api/internal/config"
)

var ErrServerClosed = http.ErrServerClosed

type Server struct {
	cfg  config.HTTP
	http *http.Server
}

func New(cfg config.HTTP, handler http.Handler) *Server {
	httpSrv := &http.Server{
		Addr:         formatAddress(cfg.Host, cfg.Port),
		Handler:      handler,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	}

	return &Server{cfg: cfg, http: httpSrv}
}

func (s *Server) Run(ctx context.Context) error {
	errCh := make(chan error, 1)

	go func() {
		errCh <- s.http.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout)
		defer cancel()
		if err := s.http.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return ErrServerClosed
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return ErrServerClosed
		}
		return err
	}
}

func formatAddress(host string, port int) string {
	return host + ":" + itoa(port)
}

func itoa(n int) string {
	return strconv.Itoa(n)
}
