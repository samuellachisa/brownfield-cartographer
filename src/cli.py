"""CLI for Brownfield Cartographer."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

from .agents.navigator import NavigatorAgent
from .graph.knowledge_graph import KnowledgeGraph
from .orchestrator import run_analysis
from .utils.logging import setup_logging
from .semantic_index import SemanticIndex
from .storage import get_cartography_dir, get_repo_cart_dir, load_state


def _repo_id_from_github_url(url: str) -> str:
    """Extract owner/repo from GitHub URL (e.g. https://github.com/owner/repo.git -> owner/repo)."""
    url = url.strip().rstrip("/")
    if url.startswith("https://github.com/"):
        parts = url.replace("https://github.com/", "").split("/")
    elif url.startswith("git@github.com:"):
        parts = url.replace("git@github.com:", "").replace(".git", "").split("/")
    else:
        return ""
    if len(parts) >= 2:
        return "/".join(parts[:2]).replace(".git", "")
    return ""


def _resolve_repo(repo: str) -> tuple[Path, str | None]:
    """Resolve repo path, optionally cloning from GitHub. Returns (path, repo_id or None)."""
    if repo.startswith("https://github.com/") or repo.startswith("git@github.com:"):
        import subprocess
        import tempfile
        clone_dir = Path(tempfile.mkdtemp(prefix="cartographer_clone_"))
        subprocess.check_call(["git", "clone", "--depth", "1", repo, str(clone_dir)])
        repo_id = _repo_id_from_github_url(repo) or None
        return clone_dir, repo_id
    return Path(repo).resolve(), None


def cmd_analyze(args: argparse.Namespace) -> None:
    repo_path, auto_repo_id = _resolve_repo(args.repo)
    repo_id = getattr(args, "repo_id", None) or auto_repo_id
    run_analysis(
        str(repo_path),
        repo_id=repo_id,
        incremental=args.incremental,
        local_only=args.local_only,
    )
    cart = get_cartography_dir(repo_path) if not repo_id else Path(__file__).resolve().parents[1] / ".cartography" / "repos" / repo_id
    print(f"Analysis complete. Artifacts: {cart}")


def _load_or_run(repo: str, force: bool = False):
    """Load cached state or run analysis. Repo can be a path or owner/repo id."""
    repo_path = Path(repo).resolve()
    # Resolve cart dir: .cartography (for repo root) or .cartography/repos/owner/repo (for repo_id)
    cart_dir = None
    if not str(repo).startswith((".", "/")) and "/" in repo and "\\" not in repo:
        candidate = get_repo_cart_dir(repo)
        if (candidate / "state.json").exists():
            cart_dir = candidate
    if cart_dir is None:
        cart_dir = repo_path if (repo_path / "state.json").exists() else get_cartography_dir(repo_path)
    if not force:
        cached = load_state(cart_dir)
        if cached:
            kg, modules, datasets, _ = cached
            idx = SemanticIndex()
            idx_path = cart_dir / "semantic_index" / "index.json"
            if not idx.load(idx_path):
                idx.build(modules)
            return kg, modules, datasets, idx
    res = run_analysis(str(repo_path), local_only=False)
    return res.graph, res.modules, res.datasets, res.semantic_index or SemanticIndex()


def cmd_query(args: argparse.Namespace) -> None:
    kg, modules, datasets, idx = _load_or_run(args.repo, force=args.force)
    nav = NavigatorAgent(semantic_index=idx)

    if args.tool == "find_implementation":
        out = nav.find_implementation(args.concept or "", modules, idx, top_k=args.top_k or 10)
    elif args.tool == "trace_lineage":
        out = nav.trace_lineage(kg, args.dataset or "", args.direction or "downstream")
    elif args.tool == "blast_radius":
        out = nav.blast_radius(kg, args.module_path or "", modules)
    elif args.tool == "explain_module":
        out = nav.explain_module(args.module_path or "", modules)
    elif args.tool == "sources":
        out = {"answer": sorted(kg.find_sources())}
    elif args.tool == "sinks":
        out = {"answer": sorted(kg.find_sinks())}
    elif args.tool == "what_breaks":
        out = kg.what_breaks_if_table_changes(args.dataset or args.table or "")
    elif args.tool == "upstream":
        out = kg.upstream_sources_for(args.dataset or args.table or "")
    elif args.tool == "impact":
        out = kg.impact_summary(args.dataset or args.table or "")
    else:
        raise SystemExit(f"Unknown tool: {args.tool}")

    print(json.dumps(out, indent=2, default=str))


def _route_ask(
    question: str,
    kg: KnowledgeGraph,
    modules: dict,
    nav: NavigatorAgent,
    idx: SemanticIndex,
) -> dict:
    """Route natural language question to the appropriate Navigator tool."""
    q = question.lower().strip()
    # "what produces X" / "upstream of X" / "what feeds X"
    if "produce" in q or "upstream" in q or "feed" in q or "source" in q:
        words = question.replace("?", "").split()
        for i, w in enumerate(words):
            if w.lower() in ("of", "for", "to") and i + 1 < len(words):
                ds = words[i + 1].strip("'\"").replace("`", "")
                return nav.trace_lineage(kg, ds, "upstream")
        for ds in list(kg.find_sources())[:5]:
            if ds.split(".")[-1] in q or ds in q:
                return nav.trace_lineage(kg, ds, "upstream")
    # "what consumes X" / "downstream of X"
    if "consum" in q or "downstream" in q:
        words = question.replace("?", "").split()
        for i, w in enumerate(words):
            if w.lower() in ("of", "for") and i + 1 < len(words):
                ds = words[i + 1].strip("'\"").replace("`", "")
                return nav.trace_lineage(kg, ds, "downstream")
    # "blast radius of X" / "what breaks if X" - try table (lineage) first, then module
    if "blast" in q or "break" in q:
        for ds in list(kg.lineage_graph.nodes()):
            if kg.lineage_graph.nodes.get(ds, {}).get("type") == "dataset" and (
                ds.lower() in q or ds.split(".")[-1].lower() in q
            ):
                return kg.impact_summary(ds)
        for path in modules:
            if path.split("/")[-1].lower() in q or path in question:
                return nav.blast_radius(kg, path, modules)
    # "explain X" / "what does X do"
    if "explain" in q or "what does" in q:
        for path in modules:
            if path.split("/")[-1].lower() in q or path in question:
                return nav.explain_module(path, modules)
    # "sources" / "sinks"
    if q == "sources":
        return {"answer": sorted(kg.find_sources())}
    if q == "sinks":
        return {"answer": sorted(kg.find_sinks())}
    # Default: semantic search
    return nav.find_implementation(question, modules, idx, top_k=5)


def cmd_shell(args: argparse.Namespace) -> None:
    """Single interactive shell: analyze, ask, find, lineage, blast, explain, sources, sinks."""
    repo_path = _resolve_repo(args.repo or ".")
    kg, modules, datasets, idx = _load_or_run(str(repo_path), force=args.force)
    nav = NavigatorAgent(semantic_index=idx)
    help_msg = (
        "Commands: analyze [--incremental] [--local-only] | ask \"<question>\" | "
        "find <concept> | lineage <dataset> [up|down] | blast <path> | explain <path> | "
        "what_breaks <table> | upstream <table> | impact <table> | sources | sinks | quit"
    )
    print(f"Cartographer shell @ {repo_path}\n{help_msg}\n")
    while True:
        try:
            line = input("> ").strip()
        except EOFError:
            break
        if not line or line in ("q", "quit", "exit"):
            break
        parts = line.split(maxsplit=1)
        cmd = parts[0].lower()
        arg = parts[1].strip() if len(parts) > 1 else ""
        if cmd == "analyze":
            inc = "--incremental" in line
            local = "--local-only" in line
            run_analysis(str(repo_path), incremental=inc, local_only=local)
            kg, modules, datasets, idx = _load_or_run(str(repo_path), force=True)
            nav = NavigatorAgent(semantic_index=idx)
            print("Analysis complete.")
        elif cmd == "ask":
            if not arg:
                print("Usage: ask \"<your question>\"")
                continue
            question = arg.strip("'\"").strip()
            out = _route_ask(question, kg, modules, nav, idx)
            print(json.dumps(out, indent=2, default=str))
        elif cmd == "find":
            out = nav.find_implementation(arg, modules, idx)
            print(json.dumps(out.get("answer", []), indent=2, default=str))
        elif cmd == "lineage":
            sub = arg.split()
            ds = sub[0] if sub else ""
            direction = "upstream" if len(sub) > 1 and sub[1].lower() == "up" else "downstream"
            out = nav.trace_lineage(kg, ds, direction)
            print(json.dumps(out, indent=2, default=str))
        elif cmd == "blast":
            out = nav.blast_radius(kg, arg, modules)
            print(json.dumps(out, indent=2, default=str))
        elif cmd == "explain":
            out = nav.explain_module(arg, modules)
            print(out.get("answer", ""))
        elif cmd == "sources":
            print(list(kg.find_sources()))
        elif cmd == "sinks":
            print(list(kg.find_sinks()))
        elif cmd == "what_breaks":
            out = kg.what_breaks_if_table_changes(arg)
            print(json.dumps(out, indent=2, default=str))
        elif cmd == "upstream":
            out = kg.upstream_sources_for(arg)
            print(json.dumps(out, indent=2, default=str))
        elif cmd == "impact":
            out = kg.impact_summary(arg)
            print(json.dumps(out, indent=2, default=str))
        else:
            print(help_msg)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="cartographer")
    sub = parser.add_subparsers(dest="command")

    # Shell: default. cartographer [repo] or cartographer shell [repo]
    p_shell = sub.add_parser("shell", help="Interactive shell with analyze & ask (default)")
    p_shell.add_argument("repo", nargs="?", default=".", help="Path to repo")
    p_shell.add_argument("--force", action="store_true", help="Re-run analysis on entry")
    p_shell.set_defaults(func=cmd_shell)

    p_analyze = sub.add_parser("analyze", help="One-off analysis (for scripts)")
    p_analyze.add_argument("repo", help="Path to repo or GitHub URL")
    p_analyze.add_argument("--repo-id", dest="repo_id", help="Unique repo identifier for per-repo storage")
    p_analyze.add_argument("--incremental", action="store_true")
    p_analyze.add_argument("--local-only", action="store_true")
    p_analyze.set_defaults(func=cmd_analyze)

    # Query: non-interactive Navigator tools (for scripts / CI)
    p_query = sub.add_parser("query", help="Run Navigator tools (find, lineage, blast, explain, sources, sinks)")
    p_query.add_argument("repo", help="Path to repo")
    p_query.add_argument(
        "tool",
        choices=[
            "find_implementation",
            "trace_lineage",
            "blast_radius",
            "explain_module",
            "sources",
            "sinks",
            "what_breaks",
            "upstream",
            "impact",
        ],
        help="Navigator tool to run",
    )
    p_query.add_argument("--concept", help="Concept to search for (find_implementation)")
    p_query.add_argument("--dataset", "--table", dest="dataset", help="Dataset/table name (trace_lineage, what_breaks, upstream, impact)")
    p_query.add_argument(
        "--direction",
        choices=["upstream", "downstream"],
        help="Lineage direction (trace_lineage)",
    )
    p_query.add_argument("--module-path", dest="module_path", help="Module path (blast_radius, explain_module)")
    p_query.add_argument("--top-k", dest="top_k", type=int, help="Top-k results (find_implementation)")
    p_query.add_argument(
        "--force",
        action="store_true",
        help="Force re-analysis instead of loading cached state",
    )
    p_query.set_defaults(func=cmd_query)

    return parser


def main(argv: list[str] | None = None) -> None:
    import sys

    setup_logging()
    args_list = (argv if argv is not None else sys.argv[1:])[:]
    # Shortcut: cartographer /path -> shell with repo
    if args_list and args_list[0] not in ("shell", "analyze", "query", "-h", "--help") and not args_list[0].startswith("-"):
        cmd_shell(argparse.Namespace(repo=args_list[0], force="--force" in args_list))
        return
    parser = build_parser()
    args = parser.parse_args(args_list if args_list else ["shell", "."])
    if hasattr(args, "repo") and args.repo is None:
        args.repo = "."
    args.func(args)


if __name__ == "__main__":
    main()
