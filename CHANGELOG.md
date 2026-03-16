# Changelog

## 1.0.3 - 2026-03-15

This patch release closes the hot/cold partitioning branch as the next `1.0.x` cut. `1.1.0` remains the planned first public release stream.

### Changed

- Unified flip storage is now the default read path.
- Legacy flip writes are disabled by default while dual-write support remains available for parity and rollback workflows.
- `/api/status` is now a local-only health endpoint suitable for uptime monitoring.
- Runtime docs and API reference were refreshed to match the current endpoint surface and storage defaults.

### Fixed

- Reduced adaptive polling heap pressure by avoiding duplicate full snapshot construction on adaptive commit paths.
- Optimized `GET /api/v1/market/overview` to use lightweight Bazaar snapshot history and unified current-flip summaries instead of loading full raw snapshot windows.
- Optimized `GET /api/v1/dashboard/overview` to use snapshot summary metadata, latest Bazaar rows, and unified current-flip summaries.
- Removed the actuator health-path dependency on an external Hypixel API call.

### Operational Notes

- This line is intended as a stabilization/homelab release, not the first public release candidate.
- Remaining `1.1.0` work is primarily release hardening, caching, observability, and unresolved shard/fusion coverage.
