[Unreleased]
### Added
- `sessions list --verbose` / `-v` flag: show token counts (input/output) and cost in table.
- `sessions list --all` flag: explicitly show all sources (including cron, tool); default now shows only interactive CLI sessions.
- `exclude_sources` parameter in `SessionDB.list_sessions_rich()` to filter out unwanted sources (cron/tool by default).

### Changed
- Default `hermes sessions list` now excludes cron and tool sessions to focus on interactive CLI sessions.
- `browse` command also excludes cron/tool by default for consistency.
- Duration formatter: durations > 1 minute now show seconds if remainder >= 30 (e.g., "2m 30s").
[Unreleased]
### Fixed
- **Agent TUI spinner always visible**: Fixed missing f-prefix on line 7670 of `cli.py` — agent placeholder was a literal string instead of f-string, so spinner frame never rendered.
- **Spinner state wiring**: `_spinner_text` (set by thinking callback) now overrides the bare spinner frame in `_get_placeholder`, so users see "🔧 tool1, tool2..." during tool execution instead of just braille characters.
- **Sessions list filtering**: `sessions list` now excludes cron/tool by default, shows only interactive CLI sessions.

### Added
- `sessions list --verbose` / `-v` flag: show token counts and cost.
- `sessions list --all` flag: show all sources.
- Session list compact view: dynamic column sizing, source column, turn/tool/error counts.





# Changelog
