# Local Herdr Codex entry point

Install the executable `codex` as a regular file at `bin/codex` in the local Herdr client package. The package's `native/` directory must contain the complete native Codex distribution. It can be a symlink to a pinned, immutable release. A package-root `codex` symlink may point to `bin/codex`.

The launcher passes the verified foreground pane, tab, workspace and inherited socket into the local TUI session's shell environment policy. This gives the ordinary SessionStart hook the TUI's pane context even when a shared app server was started from a different pane. The session UUID comes from Codex. The launcher preserves the native process PID through exec and does not register a session itself. If it cannot identify an interactive Herdr launch, it disables the Herdr hook for that invocation. Run the focused checks with `python3 scripts/herdr/test_launch.py`.
