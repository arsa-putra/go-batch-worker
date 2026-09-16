package worker

import (
	"encoding/json"
	"html/template"
	"net/http"
	"strconv"
	"strings"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

// RenderDashboardList renders the HTML page listing ALL recorded batches.
func RenderDashboardList(w http.ResponseWriter, tracker BatchTracker) {
	batches, err := tracker.ListBatches()
	if err != nil {
		http.Error(w, "Failed to load batches", http.StatusInternalServerError)
		return
	}

	// Wrap data to pass multiple variables to the template
	data := map[string]interface{}{
		"JobName": "", // Empty for global list
		"Batches": batches,
	}

	tmpl, _ := template.New("list").Parse(dashboardListHTML)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = tmpl.Execute(w, data)
}

// RenderDashboardListByJobName renders the HTML page listing recorded batches for a specific job.
func RenderDashboardListByJobName(w http.ResponseWriter, tracker BatchTracker, jobName string) {
	batches, err := tracker.LoadBatchesByJob(jobName)
	if err != nil {
		http.Error(w, "Failed to load batches", http.StatusInternalServerError)
		return
	}

	// Wrap data to include the specific job name
	caser := cases.Title(language.English)
	formattedName := caser.String(strings.ReplaceAll(jobName, "_", " "))

	data := map[string]interface{}{
		"JobName": formattedName,
		"Batches": batches,
	}

	tmpl, _ := template.New("list").Parse(dashboardListHTML)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = tmpl.Execute(w, data)
}

// RenderDashboardDetail renders the HTML detail view for a specific batch.
func RenderDashboardDetail(w http.ResponseWriter, tracker BatchTracker, batchID string) {
	result, err := tracker.BuildResult(batchID)
	if err != nil {
		http.Error(w, "Batch not found", http.StatusNotFound)
		return
	}

	tmpl, _ := template.New("detail").Parse(dashboardDetailHTML)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = tmpl.Execute(w, result)
}

// paginateSlice is a generic helper function to slice an array for server-side pagination.
func paginateSlice[T any](items []T, page, limit int) ([]T, int) {
	total := len(items)
	if total == 0 {
		return items, 0
	}

	start := (page - 1) * limit
	if start >= total {
		return []T{}, total
	}

	end := start + limit
	if end > total {
		end = total
	}

	return items[start:end], total
}

// ServeDashboardListJSON serves the list of ALL batches in JSON format with pagination.
func ServeDashboardListJSON(w http.ResponseWriter, r *http.Request, tracker BatchTracker) {
	batches, err := tracker.ListBatches()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if page < 1 {
		page = 1
	}
	if limit < 1 {
		limit = 20
	}

	paginatedBatches, total := paginateSlice(batches, page, limit)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"data": paginatedBatches,
		"meta": map[string]int{
			"page":  page,
			"limit": limit,
			"total": total,
		},
	})
}

// ServeDashboardListByJobNameJSON serves the list of batches for a specific job in JSON format.
func ServeDashboardListByJobNameJSON(w http.ResponseWriter, r *http.Request, tracker BatchTracker, jobName string) {
	batches, err := tracker.LoadBatchesByJob(jobName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if page < 1 {
		page = 1
	}
	if limit < 1 {
		limit = 20
	}

	paginatedBatches, total := paginateSlice(batches, page, limit)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"data": paginatedBatches,
		"meta": map[string]int{
			"page":  page,
			"limit": limit,
			"total": total,
		},
	})
}

// ServeDashboardDetailJSON serves a specific batch detail in JSON format with independent item pagination.
func ServeDashboardDetailJSON(w http.ResponseWriter, r *http.Request, tracker BatchTracker, batchID string) {
	result, err := tracker.BuildResult(batchID)
	if err != nil {
		http.Error(w, "Batch not found", http.StatusNotFound)
		return
	}

	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if page < 1 {
		page = 1
	}
	if limit < 1 {
		limit = 20
	}

	paginatedFailed, totalFailed := paginateSlice(result.FailedItems, page, limit)
	paginatedSuccess, totalSuccess := paginateSlice(result.SuccessItems, page, limit)

	result.FailedItems = paginatedFailed
	result.SuccessItems = paginatedSuccess

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"batch_info": result,
		"meta": map[string]interface{}{
			"page":                page,
			"limit":               limit,
			"total_failed_items":  totalFailed,
			"total_success_items": totalSuccess,
		},
	})
}
