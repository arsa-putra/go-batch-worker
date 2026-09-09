# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0-v1.0.0.html).

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