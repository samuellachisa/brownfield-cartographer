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
#   --repo-id       Unique repo identifier for per-repo storage
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
repo_id=""

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
    --repo-id)
      repo_id="$2"
      shift 2
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

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

# Ensure env is set up
if [ ! -d ".venv" ]; then
  if command -v uv >/dev/null 2>&1; then
    uv sync
  else
    echo "No .venv found and 'uv' is not installed. Please create a virtualenv and install deps first." >&2
    echo "For example:" >&2
    echo "  python -m venv .venv" >&2
    echo "  source .venv/bin/activate" >&2
    echo "  pip install -e ." >&2
    exit 1
  fi
fi

# Build CLI args
CLI_ARGS=("analyze" "$REPO")
[[ "$incremental" == "true" ]] && CLI_ARGS+=("--incremental")
[[ "$local_only" == "true" ]] && CLI_ARGS+=("--local-only")
[[ -n "$repo_id" ]] && CLI_ARGS+=("--repo-id" "$repo_id")

# Always invoke via the module so we don't rely on the broken console script.
if command -v uv >/dev/null 2>&1; then
  uv run python -m src.cli "${CLI_ARGS[@]}"
else
  source .venv/bin/activate
  python -m src.cli "${CLI_ARGS[@]}"
fi

