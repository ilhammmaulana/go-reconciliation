package api

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"go-reconciliation/internal/config"
	"go-reconciliation/internal/domain"
	"go-reconciliation/internal/logger"
	"go-reconciliation/internal/worker"
)

type Handler struct {
	pool   *worker.WorkerPool
	config *config.Config
}

func NewHandler(pool *worker.WorkerPool, cfg *config.Config) *Handler {
	return &Handler{pool: pool, config: cfg}
}

func (h *Handler) RegisterRoutes(r *gin.Engine) {
	r.GET("/healthz", h.HealthCheck)

	api := r.Group("/api/v1")
	{
		grouping := api.Group("/grouping")
		{
			grouping.POST("/rerun", h.TriggerRerun)
			grouping.POST("/trigger", h.TriggerProject)
		}

		// Log management endpoints protected by API_KEY
		logs := api.Group("/logs")
		{
			logs.POST("/clear", h.ClearAllLogs)
			logs.DELETE("/clear", h.ClearAllLogs)
		}
	}
}

func (h *Handler) HealthCheck(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":  "healthy",
		"service": "go-reconciliation",
	})
}

func (h *Handler) TriggerRerun(c *gin.Context) {
	var req domain.JobPayload
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	req.Action = "RERUN_FILE"
	h.pool.EnqueueJob(req)

	c.JSON(http.StatusAccepted, gin.H{
		"status":  "accepted",
		"message": "File rerun job enqueued",
		"payload": req,
	})
}

func (h *Handler) TriggerProject(c *gin.Context) {
	var req domain.JobPayload
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	req.Action = "GROUP_PROJECT"
	h.pool.EnqueueJob(req)

	c.JSON(http.StatusAccepted, gin.H{
		"status":  "accepted",
		"message": "Project grouping job enqueued",
		"payload": req,
	})
}

// ClearAllLogs truncates all logs in the logs/ directory after validating API_KEY
func (h *Handler) ClearAllLogs(c *gin.Context) {
	expectedKey := ""
	if h.config != nil {
		expectedKey = h.config.ApiKey
	}
	if expectedKey == "" {
		expectedKey = "secret-reconcile-key"
	}

	// 1. Check API Key from header or query param
	apiKey := c.GetHeader("X-API-KEY")
	if apiKey == "" {
		authHeader := c.GetHeader("Authorization")
		if strings.HasPrefix(authHeader, "Bearer ") {
			apiKey = strings.TrimPrefix(authHeader, "Bearer ")
		}
	}
	if apiKey == "" {
		apiKey = c.Query("api_key")
	}

	if apiKey != expectedKey {
		c.JSON(http.StatusUnauthorized, gin.H{
			"status":  "error",
			"message": "Unauthorized: invalid or missing API key",
		})
		return
	}

	// 2. Clear logs
	cleared, err := logger.ClearLogs()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": "Failed to clear logs: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":        "success",
		"message":       "All logs cleared successfully",
		"cleared_files": cleared,
	})
}
