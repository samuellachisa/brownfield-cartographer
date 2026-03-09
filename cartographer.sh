#!/usr/bin/env bash
set -euo pipefail

# Brownfield Cartographer one-stop script
# Usage:
#   ./cartographer.sh [options] <repo-or-github-url>
#
# Options:
#   --incremental   Analyze only changed files (requires git)
#   --local-only    Skip LLM (static analysis only)
#   --analyze-only  Do not start the interactive shell after analysis
#   --force         Re-run analysis even if cache exists
#
# Examples:
#   ./cartographer.sh .                          # analyze + shell on current repo
#   ./cartographer.sh ../my-repo                 # local path
#   ./cartographer.sh https://github.com/org/repo  # GitHub URL
#   ./cartographer.sh --incremental .            # incremental analysis + shell
#   ./cartographer.sh --local-only .             # static-only analysis + shell

incremental=false
local_only=false
analyze_only=false
force=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --incremental)
      incremental=true
      shift
      ;;
    --local-only)
      local_only=true
      shift
      ;;
    --analyze-only)
      analyze_only=true
      shift
      ;;
    --force)
      force=true
      shift
      ;;
    -*)
      echo "Unknown option: $1" >&2
      exit 1
      ;;
    *)
      REPO="$1"
      shift
      ;;
  esac
done

if [[ -z "${REPO:-}" ]]; then
  echo "Usage: $0 [options] <repo-or-github-url>" >&2
  exit 1
fi

# Ensure cartographer is on PATH
if ! command -v cartographer >/dev/null 2>&1; then
  echo "cartographer CLI not found. Install this project from the root with:" >&2
  echo "  pip install -e .    # or: pip install -r requirements.txt" >&2
  exit 1
fi

# Build analyze command
analyze_cmd=(cartographer analyze "$REPO")
$incremental && analyze_cmd+=("--incremental")
$local_only && analyze_cmd+=("--local-only")

echo ">>> Running analysis: ${analyze_cmd[*]}"
"${analyze_cmd[@]}"

if $analyze_only; then
  echo ">>> Analysis complete. Artifacts are in .cartography/ of the repo."
  exit 0
fi

# For GitHub URLs, the CLI clones to a temp dir; for simplicity, drop into shell
# on current working directory if REPO is not a local path.
if [[ "$REPO" == https://github.com/* || "$REPO" == git@github.com:* ]]; then
  SHELL_REPO="."
else
  SHELL_REPO="$REPO"
fi

shell_cmd=(cartographer shell "$SHELL_REPO")
$force && shell_cmd+=("--force")

echo ">>> Starting interactive shell: ${shell_cmd[*]}"
"${shell_cmd[@]}"

