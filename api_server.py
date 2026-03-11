#!/usr/bin/env python3
"""
Simple API server to serve cartography data to the UI.
"""
import json
import subprocess
import tempfile
import shutil
import threading
from datetime import datetime
from pathlib import Path
from flask import Flask, jsonify, request
from flask_cors import CORS

from src.agents.navigator import NavigatorAgent
from src.semantic_index import SemanticIndex
from src.storage import get_repo_cart_dir, load_state
from src.agents.semanticist import LLMClient, LLMConfig

app = Flask(__name__)
CORS(app)

CARTOGRAPHY_DIR = Path(__file__).parent / ".cartography"
REPOS_FILE = CARTOGRAPHY_DIR / "repositories.json"


def load_repositories():
    """Load the list of analyzed repositories."""
    if REPOS_FILE.exists():
        return json.loads(REPOS_FILE.read_text())
    return {"repositories": []}


def save_repositories(repos_data):
    """Save the list of analyzed repositories."""
    CARTOGRAPHY_DIR.mkdir(exist_ok=True)
    REPOS_FILE.write_text(json.dumps(repos_data, indent=2))


def update_repository_status(repo_id: str, status: str, error: str | None = None) -> None:
    """Update a single repository's status.

    - On completion, updates last_analyzed timestamp.
    - On failure, records a last_error message (if provided).
    """
    try:
        repos_data = load_repositories()
        repo = next((r for r in repos_data["repositories"] if r.get("id") == repo_id), None)
        if repo:
            repo["status"] = status
            if error:
                repo["last_error"] = error
            if status == "completed":
                repo["last_analyzed"] = datetime.utcnow().isoformat()
                # Ensure cartography_path is set on completion if missing
                if not repo.get("cartography_path"):
                    repo["cartography_path"] = f".cartography/repos/{repo_id}"
        save_repositories(repos_data)
    except Exception:
        # Best-effort; don't crash background threads on failure.
        pass


def monitor_analysis_process(process: subprocess.Popen, repo_id: str) -> None:
    """Wait for the analysis process to finish and update repo status."""
    try:
        stdout, stderr = process.communicate()
        return_code = process.returncode
        if return_code == 0:
            update_repository_status(repo_id, "completed", error=None)
        else:
            # Store a short snippet of stderr for debugging in the UI.
            snippet = (stderr or "").strip()
            if len(snippet) > 2000:
                snippet = snippet[:2000] + "... (truncated)"
            update_repository_status(
                repo_id,
                "failed",
                error=snippet or f"Analysis exited with code {return_code}",
            )
    except Exception:
        update_repository_status(repo_id, "failed", error="Analysis process crashed")


def clone_github_repo(github_url: str) -> str:
    """Clone a GitHub repository to a temporary directory."""
    # Clean up the URL - remove any trailing slashes or extra paths
    github_url = github_url.strip().rstrip('/')
    
    # Create a temporary directory
    temp_dir = tempfile.mkdtemp(prefix="cartographer_")
    
    try:
        # Clone the repository
        result = subprocess.run(
            ["git", "clone", "--depth", "1", github_url, temp_dir],
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout
        )
        
        if result.returncode != 0:
            shutil.rmtree(temp_dir, ignore_errors=True)
            raise Exception(f"Git clone failed: {result.stderr}")
        
        return temp_dir
    except subprocess.TimeoutExpired:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise Exception("Repository clone timed out (5 minutes)")
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise


@app.route("/api/repositories", methods=["GET"])
def get_repositories():
    """Get list of all analyzed repositories."""
    try:
        repos_data = load_repositories()
        # Attach a URL-safe slug for frontend routing (replace '/' with '__').
        repos_with_slugs = []
        for repo in repos_data.get("repositories", []):
            repo_copy = dict(repo)
            repo_copy["slug"] = repo_copy.get("id", "").replace("/", "__")
            repos_with_slugs.append(repo_copy)
        return jsonify({"repositories": repos_with_slugs})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def get_cart_path_for_repo(repo_id: str) -> Path:
    """
    Resolve the cartography path for a given repo_id.

    Prefers the stored cartography_path in repositories.json and falls back
    to the default .cartography/repos/<repo_id> location.
    """
    repos_data = load_repositories()
    repo = next((r for r in repos_data["repositories"] if r["id"] == repo_id), None)
    if not repo:
        raise ValueError(f"Unknown repo_id: {repo_id}")
    cart_path = repo.get("cartography_path")
    if not cart_path:
        return get_repo_cart_dir(repo_id)
    return Path(cart_path).resolve()


@app.route("/api/overview", methods=["GET"])
def get_overview():
    """Get overview data including day-one brief and repo summary."""
    try:
        repo_id = request.args.get("repo_id")
        if not repo_id:
            return jsonify({"error": "repo_id query parameter is required"}), 400

        cart_path = get_cart_path_for_repo(repo_id)
        repos_data = load_repositories()
        repo_meta = next(
            (r for r in repos_data.get("repositories", []) if r.get("id") == repo_id),
            {},
        )

        # Load onboarding brief
        brief_path = cart_path / "onboarding_brief.md"
        brief_content = brief_path.read_text() if brief_path.exists() else ""
        
        # Load state for module/dataset counts and risk heuristics
        state_path = cart_path / "state.json"
        state = json.loads(state_path.read_text()) if state_path.exists() else {}
        
        # Load runs
        runs_path = cart_path / "runs.json"
        runs_data = json.loads(runs_path.read_text()) if runs_path.exists() else {"runs": []}
        
        # Count modules and datasets and compute "risky" modules.
        module_count = 0
        dataset_count = 0
        risky_modules = []
        if "graph" in state and "module_graph" in state["graph"]:
            nodes = state["graph"]["module_graph"].get("nodes", [])
            # Only consider true modules (filter out import sentinel nodes, etc.).
            module_nodes = [
                n
                for n in nodes
                if isinstance(n, dict) and n.get("type") == "module"
            ]
            module_count = len(module_nodes)

            # Simple risk heuristic: combine static complexity and recent change velocity.
            scored = []
            for n in module_nodes:
                try:
                    complexity = float(n.get("complexity_score", 0.0) or 0.0)
                except (TypeError, ValueError):
                    complexity = 0.0
                try:
                    velocity = float(n.get("change_velocity_30d", 0) or 0.0)
                except (TypeError, ValueError):
                    velocity = 0.0

                # Weight recent change velocity slightly higher than raw complexity.
                risk_score = complexity + 10.0 * velocity

                # De-emphasize modules that already look like dead code.
                if n.get("is_dead_code_candidate"):
                    risk_score *= 0.5

                scored.append((risk_score, n))

            scored.sort(key=lambda t: t[0], reverse=True)
            top = [n for score, n in scored[:10] if score > 0]
            risky_modules = [
                {
                    "id": n.get("id"),
                    "complexity_score": n.get("complexity_score", 0.0),
                    "change_velocity_30d": n.get("change_velocity_30d", 0),
                    "is_dead_code_candidate": n.get("is_dead_code_candidate", False),
                }
                for n in top
                if n.get("id")
            ]
        
        if "graph" in state and "lineage_graph" in state["graph"]:
            nodes = state["graph"]["lineage_graph"].get("nodes", [])
            dataset_count = sum(1 for n in nodes if isinstance(n, dict) and n.get("type") == "dataset")
        
        last_run = runs_data["runs"][-1] if runs_data["runs"] else None
        
        return jsonify(
            {
                "brief": brief_content,
                "summary": {
                    "modules": module_count,
                    "datasets": dataset_count,
                    "last_run": last_run["timestamp"] if last_run else None,
                    "repo_path": last_run["repo_path"] if last_run else None,
                },
                "risky_modules": risky_modules,
                "status": repo_meta.get("status"),
                "last_error": repo_meta.get("last_error"),
            }
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/runs", methods=["GET"])
def get_runs():
    """Get all analysis runs."""
    try:
        repo_id = request.args.get("repo_id")
        if not repo_id:
            return jsonify({"error": "repo_id query parameter is required"}), 400

        cart_path = get_cart_path_for_repo(repo_id)
        runs_path = cart_path / "runs.json"
        if runs_path.exists():
            runs_data = json.loads(runs_path.read_text())
            return jsonify(runs_data)
        return jsonify({"runs": []})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/modules", methods=["GET"])
def get_modules():
    """Get module graph data."""
    try:
        repo_id = request.args.get("repo_id")
        if not repo_id:
            return jsonify({"error": "repo_id query parameter is required"}), 400

        cart_path = get_cart_path_for_repo(repo_id)
        state_path = cart_path / "state.json"
        if state_path.exists():
            state = json.loads(state_path.read_text())
            module_graph = state.get("graph", {}).get("module_graph", {})
            return jsonify(module_graph)
        return jsonify({"nodes": [], "edges": []})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/lineage", methods=["GET"])
def get_lineage():
    """Get lineage graph data."""
    try:
        repo_id = request.args.get("repo_id")
        if not repo_id:
            return jsonify({"error": "repo_id query parameter is required"}), 400

        cart_path = get_cart_path_for_repo(repo_id)
        state_path = cart_path / "state.json"
        if state_path.exists():
            state = json.loads(state_path.read_text())
            lineage_graph = state.get("graph", {}).get("lineage_graph", {})
            return jsonify(lineage_graph)
        return jsonify({"nodes": [], "edges": []})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/ask", methods=["POST"])
def ask_repo():
    """
    Simple Q&A endpoint scoped to a repo.

    Body: { repo_id: str, question: str }
    Returns: { answer: str, evidence: list[str] }
    """
    try:
        data = request.get_json(force=True) or {}
        repo_id = data.get("repo_id")
        question = data.get("question", "").strip()

        if not repo_id or not question:
            return jsonify({"error": "repo_id and question are required"}), 400

        # Load per-repo analysis state and semantic index.
        cart_path = get_cart_path_for_repo(repo_id)
        state = load_state(cart_path)
        if not state:
            return jsonify(
                {
                    "answer": f"No analysis state found for repo '{repo_id}'. "
                    "Run an analysis before asking questions.",
                    "evidence": [],
                }
            )

        kg, modules, datasets, _day_one_answers = state

        idx = SemanticIndex()
        idx_path = cart_path / "semantic_index" / "index.json"
        if not idx.load(idx_path):
            # If the index wasn't saved for some reason, rebuild it in-process.
            idx.build(modules)

        nav = NavigatorAgent(semantic_index=idx)

        # Reuse the same routing logic as the CLI shell.
        q = question.lower().strip()
        if "produce" in q or "upstream" in q or "feed" in q or "source" in q:
            words = question.replace("?", "").split()
            for i, w in enumerate(words):
                if w.lower() in ("of", "for", "to") and i + 1 < len(words):
                    ds = words[i + 1].strip("'\"").replace("`", "")
                    out = nav.trace_lineage(kg, ds, "upstream")
                    break
            else:
                chosen = None
                for ds in list(kg.find_sources())[:5]:
                    if ds.split(".")[-1] in q or ds in q:
                        chosen = ds
                        break
                if chosen:
                    out = nav.trace_lineage(kg, chosen, "upstream")
                else:
                    out = nav.find_implementation(question, modules, idx, top_k=5)
        elif "consum" in q or "downstream" in q:
            words = question.replace("?", "").split()
            ds = ""
            for i, w in enumerate(words):
                if w.lower() in ("of", "for") and i + 1 < len(words):
                    ds = words[i + 1].strip("'\"").replace("`", "")
                    break
            out = nav.trace_lineage(kg, ds, "downstream") if ds else nav.find_implementation(question, modules, idx, top_k=5)
        elif "blast" in q or "break" in q:
            target = ""
            for path in modules:
                if path.split("/")[-1].lower() in q or path in question:
                    target = path
                    break
            out = nav.blast_radius(kg, target, modules) if target else nav.find_implementation(question, modules, idx, top_k=5)
        elif "explain" in q or "what does" in q:
            target = ""
            for path in modules:
                if path.split("/")[-1].lower() in q or path in question:
                    target = path
                    break
            out = nav.explain_module(target, modules) if target else nav.find_implementation(question, modules, idx, top_k=5)
        elif q == "sources":
            out = {"answer": sorted(kg.find_sources())}
        elif q == "sinks":
            out = {"answer": sorted(kg.find_sinks())}
        else:
            out = nav.find_implementation(question, modules, idx, top_k=5)

        # Normalize Navigator output into { answer, evidence }.
        answer_text = None
        evidence = []
        if isinstance(out, dict):
            if "answer" in out:
                answer_text = out["answer"]
            if "evidence" in out and isinstance(out["evidence"], list):
                evidence = out["evidence"]

        # If an LLM is configured, synthesize a natural-language answer that
        # references the navigator's evidence instead of returning raw objects.
        try:
            cfg = LLMConfig()
            if cfg.api_key:
                client = LLMClient(cfg)
                # Build a compact textual context for the LLM.
                context_lines: list[str] = []
                if isinstance(out, dict):
                    oa = out.get("answer")
                    if isinstance(oa, list):
                        for item in oa[:5]:
                            if not isinstance(item, dict):
                                continue
                            mpath = item.get("module_path") or item.get("id") or item.get("name")
                            purpose = item.get("purpose_statement") or ""
                            score = item.get("score")
                            line = f"- {mpath}"
                            if isinstance(score, (int, float)):
                                line += f" (score {score:.2f})"
                            if purpose:
                                line += f": {purpose}"
                            context_lines.append(line)
                    elif isinstance(oa, dict):
                        # Lineage / blast-radius style answers.
                        direction = oa.get("direction")
                        nodes = oa.get("nodes") or []
                        if direction:
                            context_lines.append(f"Lineage direction: {direction}.")
                        for n in nodes[:10]:
                            if not isinstance(n, dict):
                                continue
                            nid = n.get("id")
                            ntype = n.get("type")
                            context_lines.append(f"- {nid} ({ntype})")
                context = "\n".join(context_lines) if context_lines else str(out)

                prompt = (
                    "You are Brownfield Cartographer's Navigator, a senior data platform engineer.\n"
                    "The user has asked a question about a data platform repository.\n"
                    "Based on the provided context (modules, datasets, lineage), answer in 2-4 concise sentences.\n"
                    "Focus on explaining the likely ingestion or transformation path in clear, non-hype language.\n\n"
                    f"Question:\n{question}\n\n"
                    f"Context:\n{context}\n"
                )
                client._rate_limiter.wait()
                resp = client._client.chat.completions.create(
                    model=client.config.model,
                    messages=[
                        {"role": "system", "content": "You are a senior data platform engineer."},
                        {"role": "user", "content": prompt},
                    ],
                    max_tokens=400,
                    temperature=0.2,
                )
                llm_answer = (resp.choices[0].message.content or "").strip()
                if llm_answer:
                    answer_text = llm_answer
        except Exception:
            # If LLM integration fails for any reason, silently fall back to structured output.
            pass

        if answer_text is None:
            # Fallback: serialize the whole object.
            try:
                answer_text = json.dumps(out, indent=2, default=str)
            except Exception:
                answer_text = str(out)

        return jsonify({"answer": answer_text, "evidence": evidence})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/analyze", methods=["POST"])
def trigger_analysis():
    """Trigger analysis on a GitHub repository."""
    try:
        data = request.get_json()
        github_url = data.get("github_url")
        
        if not github_url:
            return jsonify({"error": "github_url is required"}), 400
        
        # Validate it's a GitHub URL
        if not (github_url.startswith("https://github.com/") or github_url.startswith("git@github.com:")):
            return jsonify({"error": "Only GitHub URLs are supported (https://github.com/... or git@github.com:...)"}), 400
        
        # Extract repo name from URL
        repo_name = github_url.rstrip('/').split('/')[-1].replace('.git', '')
        repo_owner = github_url.rstrip('/').split('/')[-2]
        repo_id = f"{repo_owner}/{repo_name}"
        
        # Add to repositories list
        repos_data = load_repositories()
        existing_repo = next((r for r in repos_data["repositories"] if r["id"] == repo_id), None)
        
        if not existing_repo:
            repos_data["repositories"].append({
                "id": repo_id,
                "name": repo_name,
                "owner": repo_owner,
                "github_url": github_url,
                "status": "analyzing",
                "created_at": None,
                "last_analyzed": None,
                "cartography_path": f".cartography/repos/{repo_id}",
            })
            save_repositories(repos_data)
        else:
            # Update status to analyzing
            existing_repo["status"] = "analyzing"
            save_repositories(repos_data)
        
        # Clone the repository
        try:
            repo_path = clone_github_repo(github_url)
        except Exception as e:
            # Update status and persist the error message
            update_repository_status(
                repo_id,
                "failed",
                error=f"Failed to clone repository: {str(e)}",
            )
            return jsonify({"error": f"Failed to clone repository: {str(e)}"}), 400
        
        # Run the cartographer script with repo_id
        script_path = Path(__file__).parent / "cartographer.sh"

        # Start the analysis in the background
        process = subprocess.Popen(
            [str(script_path), "analyze", repo_path, "--repo-id", repo_id],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        # Monitor the process in a background thread and update repo status when done
        threading.Thread(
            target=monitor_analysis_process, args=(process, repo_id), daemon=True
        ).start()

        return jsonify({
            "message": "Analysis started",
            "repo_id": repo_id,
            "github_url": github_url,
            "temp_path": repo_path,
            "pid": process.pid
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
