package handlers

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/arori/job-scheduler/internal/models"
	"github.com/arori/job-scheduler/internal/store"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type JobsHandler struct {
	Repo *store.JobsRepo
}

func NewJobsHandler(repo *store.JobsRepo) *JobsHandler {
	return &JobsHandler{Repo: repo}
}

// CreateJob handles POST /jobs
func (h *JobsHandler) CreateJob(w http.ResponseWriter, r *http.Request) {
	var req models.CreateJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.IdempotencyKey == "" || req.JobType == "" || req.TenantID == "" {
		writeError(w, http.StatusBadRequest, "idempotencyKey, jobType, and tenantId are required")
		return
	}

	job, err := h.Repo.Create(r.Context(), req)
	if errors.Is(err, store.ErrDuplicateIdempotencyKey) {
		writeError(w, http.StatusConflict, "job with this idempotency key already exists")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to create job")
		return
	}

	writeJSON(w, http.StatusCreated, job)
}

// GetJob handles GET /jobs/{id}
func (h *JobsHandler) GetJob(w http.ResponseWriter, r *http.Request, idParam string) {
	id, err := primitive.ObjectIDFromHex(idParam)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid job id")
		return
	}

	job, err := h.Repo.GetByID(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to fetch job")
		return
	}

	writeJSON(w, http.StatusOK, job)
}

// ListJobs handles GET /jobs?tenantId=...&status=...
func (h *JobsHandler) ListJobs(w http.ResponseWriter, r *http.Request) {
	tenantID := r.URL.Query().Get("tenantId")
	if tenantID == "" {
		writeError(w, http.StatusBadRequest, "tenantId query param is required")
		return
	}
	status := models.JobStatus(r.URL.Query().Get("status"))

	jobs, err := h.Repo.ListByTenant(r.Context(), tenantID, status, 100)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list jobs")
		return
	}

	writeJSON(w, http.StatusOK, jobs)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
