# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0-v1.0.0.html).

## [v1.3.0] - 2026-09-30

### Changed
- Added support for both `gopkg.in/redis.v5` clients and `go-redis/v9` clients. The internal adapter keeps context handling out of framework usage and preserves existing Redis operations.

## [v1.2.0] - 2026-09-16

### Added
- **Auto-Linking & Indexing**: Implemented zero-config job indexing. `BulkWorker` now automatically links tracking keys to their specific job name (`w.Name()`) using Redis Sets upon payload submission.
- **Job-Specific & Global Dashboards**: Added support for both global batch monitoring (`/batches`) and job-specific routing (`/batches/{job}`) with dynamic UI header rendering.
- **Advanced Dashboard Filtering**: Added client-side date range filtering (`From` and `To` date pickers) alongside real-time Batch ID search and pagination.

### Refactored
- **Codebase Standardization**: Translated all remaining Indonesian inline comments to English across `tracker.go`, `bulk_worker.go`, `batch_worker.go`, and `dashboard.go` for professional convention.
- **Memory Optimization**: Refactored dashboard list data retrieval to return a lightweight `[]*BatchState` instead of a heavy payload containing item arrays, preventing potential OOM (Out of Memory) issues on the list view.
- **Timestamp & Date Rendering**: Optimized `CreatedAt` handling by preserving raw Unix epoch timestamps in the DOM and formatting readable local dates entirely on the client side.

## [v1.1.1] - 2026-09-09

### Fixed
- **Dashboard UI**: Fixed action button text wrapping and alignment issues on the main batch list table.
- **Batch List Status**: Added a robust status fallback (`running`) for batches with missing or unrecorded status properties.
- **Date Formatting**: Improved client-side timestamp parsing robustness for better date rendering in the batch list view.

### Improved
- Polished table column widths, spacing, and padding across the batch list dashboard for a cleaner, executive-level layout.

## [v1.1.0] - 2026-09-09

### Added
- **Dashboard & Tracker**: Added automated real-time tracking for pending/processing batch items using Redis Hash and Sets.
- **UI/UX**: Added a dedicated **Processing / Waiting** tab to the batch detail dashboard view, complete with pagination and search filtering.
- **BulkWorker**: Automated `TrackPendingPayload` and cleanup hooks inside `Submit`, `CompleteSuccess`, and `CompleteFailed` methods to eliminate manual key management.
- **Generic Payload Viewer**: Included a generic raw payload view inside the processing table to support flexible, custom user structs safely.

### Improved
- Rearranged batch detail tab headers order to **Success -> Failed -> Processing** for a more intuitive monitoring workflow.
- Updated example `main.go` with automatic `FLUSHALL` on startup for easier local testing and cleaner test runs.

## [1.0.1] - 2026-09-08

### Fixed
- Synchronized `Success` and `Failed` summary counts in `BuildResult` to accurately match the length of the cleaned result lists.
- Fixed duplicate handling and cross-contamination filtering logic between success and failure tracking items.

### Refactor
- Translated all internal code comments and documentation across core tracker methods into standard English.

## [1.0.0] - 2026-09-08

### Added
- Initial public release of `go-batch-worker`, a robust Go framework for high-throughput batch logging and bulk task processing.
- Dual worker architecture supporting batch and bulk task processing operations.
- Redis-backed tracking system for managing state, progress, and execution logs.
- Automatic cleaning and deduplication logic in `BuildResult` to eliminate cross-contamination between success and failed items.
- GitHub Actions CI/CD workflow configuration for automated testing with Go 1.26 and Redis.
- Comprehensive English documentation and code comments across core modules.
