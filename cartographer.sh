#!/usr/bin/env bash
# Interactive wrapper for Brownfield Cartographer. Run with no args for menu.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PYTHON="${PYTHON:-.venv/bin/python}"
if [[ ! -x "$PYTHON" ]]; then
  echo "Error: .venv not found. Run: python -m venv .venv && .venv/bin/pip install -r requirements.txt" >&2
  exit 1
fi

run_cli() {
  if [[ "$1" == "--time" ]]; then
    shift
    time "$PYTHON" -m src.cli "$@"
  else
    exec "$PYTHON" -m src.cli "$@"
  fi
}

# If args given, pass through to CLI (analyze always timed)
if [[ $# -gt 0 ]] && [[ "$1" != "menu" ]] && [[ "$1" != "help" ]]; then
  if [[ "$1" == "analyze" ]]; then
    run_cli --time "$@"
  else
    run_cli "$@"
  fi
fi

# Interactive menu
echo ""
echo "  Brownfield Cartographer"
echo "  ======================="
echo ""
echo "  1) analyze      Run full analysis on a repo (timed)"
echo "  2) query        Run a Navigator query (lineage, blast radius, find, etc.)"
echo "  3) shell        Interactive shell (analyze & ask)"
echo "  4) help         Show usage / options"
echo "  5) exit"
echo ""
read -rp "  Select (1-5): " choice
echo ""

case "$choice" in
  1)
    read -rp "  Repo path or GitHub URL [.]: " repo
    repo=${repo:-.}
    echo "  Options: --incremental  --local-only  --repo-id ID"
    read -rp "  Extra options (space-separated, or Enter for none): " opts
    echo ""
    run_cli --time analyze "$repo" $opts
    ;;
  2)
    read -rp "  Repo path [.]: " repo
    repo=${repo:-.}
    echo ""
    echo "  Tools: find_implementation | trace_lineage | blast_radius | explain_module"
    echo "         sources | sinks | what_breaks | upstream | impact"
    read -rp "  Tool: " tool
    args=("$repo" "$tool")
    case "$tool" in
      trace_lineage|what_breaks|upstream|impact)
        read -rp "  --dataset (table name): " ds
        [[ -n "$ds" ]] && args+=(--dataset "$ds")
        if [[ "$tool" == "trace_lineage" ]]; then
          read -rp "  --direction (upstream|downstream) [downstream]: " dir
          dir=${dir:-downstream}
          args+=(--direction "$dir")
        fi
        ;;
      blast_radius|explain_module)
        read -rp "  --module-path: " mp
        [[ -n "$mp" ]] && args+=(--module-path "$mp")
        ;;
      find_implementation)
        read -rp "  --concept: " concept
        [[ -n "$concept" ]] && args+=(--concept "$concept")
        read -rp "  --top-k [10]: " topk
        [[ -n "$topk" ]] && args+=(--top-k "$topk")
        ;;
    esac
    read -rp "  --force (re-analyze)? [y/N]: " force
    [[ "$force" =~ ^[yY] ]] && args+=(--force)
    echo ""
    run_cli query "${args[@]}"
    ;;
  3)
    read -rp "  Repo path [.]: " repo
    repo=${repo:-.}
    read -rp "  --force (re-analyze on entry)? [y/N]: " force
    echo ""
    if [[ "$force" =~ ^[yY] ]]; then
      run_cli shell "$repo" --force
    else
      run_cli shell "$repo"
    fi
    ;;
  4)
    "$PYTHON" -m src.cli -h
    echo ""
    echo "  Or run: ./cartographer.sh analyze . --local-only"
    echo "          ./cartographer.sh --time analyze ."
    echo "          ./cartographer.sh query . upstream --dataset customers"
    echo "          ./cartographer.sh query . blast_radius --module-path src/orchestrator.py"
    ;;
  5)
    echo "  Bye."
    exit 0
    ;;
  *)
    echo "  Invalid choice."
    exit 1
    ;;
esac
