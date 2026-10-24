"""Execute the pinned upstream prompt builder without importing its agent stack.

Only hash-verified packaged source is parsed. The selected function AST is
unchanged: upstream imports, YAML loaders and tool implementations are not run.
"""
from __future__ import annotations

import ast
import hashlib
from pathlib import Path
import re
from types import SimpleNamespace

RESOURCE_DIR = Path(__file__).with_name("resources")
SOURCE_HASHES = {
    "upstream_prompts.py.txt": "17e1e021fd027746ac9e47c1bd31d58f4b6bcd20fa374dfce28f55904d266b87",
    "upstream_claude_solver.py.txt": "31f3b03d1d5061ff2f4ffc97371b2403c6afe0df374ca8f771bfd5e2384ee1fc",
    "upstream_coordinator.py.txt": "f90688c365429eff9a6e9ac0aad7c1733a3b651278e10c63b01df41abf080c04",
    "upstream_tools_core.py.txt": "b5d16249ce5672e36c48bcb1f7a0d798dc575121441639fc1a920efd7ef1798d",
    "upstream_output_types.py.txt": "260c051b5254cf7f9f319adfc0a6d4ca926475ba88820f80aab743e7b4e93167",
    "local_auxiliary_prompts.py.txt": "31b726f32d3a8a7b54cb27a868da32bb80f3deb2ba29d608e9605133d14e7748",
}


def source_tree(name: str) -> ast.Module:
    raw = (RESOURCE_DIR / name).read_bytes()
    if hashlib.sha256(raw).hexdigest() != SOURCE_HASHES[name]:
        raise RuntimeError(f"Prompt baseline changed without reviewed source update: {name}")
    return ast.parse(raw.decode(), filename=name)


def assignment(tree: ast.Module, name: str):
    for node in ast.walk(tree):
        targets = node.targets if isinstance(node, ast.Assign) else [node.target] if isinstance(node, ast.AnnAssign) else []
        if any(isinstance(target, ast.Name) and target.id == name for target in targets):
            return node.value
    raise RuntimeError(f"Pinned upstream assignment missing: {name}")


PROMPT_TREE = source_tree("upstream_prompts.py.txt")
CLAUDE_TREE = source_tree("upstream_claude_solver.py.txt")
IMAGE_EXTS = ast.literal_eval(assignment(source_tree("upstream_tools_core.py.txt"), "IMAGE_EXTS_FOR_VISION"))
SANDBOX_PREAMBLE = ast.literal_eval(assignment(CLAUDE_TREE, "sandbox_preamble"))
COORDINATOR_PROMPT = ast.literal_eval(assignment(source_tree("upstream_coordinator.py.txt"), "COORDINATOR_PROMPT"))
LOCAL_TREE = source_tree("local_auxiliary_prompts.py.txt")
REPORTER_PROMPT = ast.literal_eval(assignment(LOCAL_TREE, "REPORTER_PROMPT"))
REVIEWER_PROMPT = ast.literal_eval(assignment(LOCAL_TREE, "REVIEWER_PROMPT"))

_functions = [node for node in PROMPT_TREE.body if isinstance(node, ast.FunctionDef)
              and node.name in {"_rewrite_connection_info", "build_prompt"}]
if {node.name for node in _functions} != {"_rewrite_connection_info", "build_prompt"}:
    raise RuntimeError("Pinned upstream prompt functions missing")
_namespace = {"re": re, "Path": Path, "IMAGE_EXTS": IMAGE_EXTS, "ChallengeMeta": SimpleNamespace}
exec(compile(ast.Module(body=_functions, type_ignores=[]), "upstream_prompts.py.txt", "exec"), _namespace)
build_prompt = _namespace["build_prompt"]
rewrite_connection_info = _namespace["_rewrite_connection_info"]

_output_tree = source_tree("upstream_output_types.py.txt")
_output_functions = [node for node in _output_tree.body if isinstance(node, ast.FunctionDef)
                     and node.name == "solver_output_json_schema"]
if len(_output_functions) != 1:
    raise RuntimeError("Pinned upstream solver output schema missing")
_output_namespace = {}
exec(compile(ast.Module(body=_output_functions, type_ignores=[]), "upstream_output_types.py.txt", "exec"), _output_namespace)
solver_output_json_schema = _output_namespace["solver_output_json_schema"]

# Extract all three upstream continuation templates, including literal parts of
# its f-string. Never maintain a shorter parallel imitation of these strings.
_prompts = [node.value for node in ast.walk(CLAUDE_TREE) if isinstance(node, ast.Assign)
            and any(isinstance(t, ast.Name) and t.id == "prompt" for t in node.targets)]
FIRST_PROMPT = next(ast.literal_eval(node) for node in _prompts
                    if isinstance(node, ast.Constant) and node.value.startswith("Solve"))
CONTINUE_PROMPT = next(ast.literal_eval(node) for node in _prompts
                       if isinstance(node, ast.Constant) and node.value.startswith("Continue"))
_bump = next(node for node in _prompts if isinstance(node, ast.JoinedStr))
BUMP_PREFIX, BUMP_SUFFIX = [node.value for node in _bump.values if isinstance(node, ast.Constant)]
