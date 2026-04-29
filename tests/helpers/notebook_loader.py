import ast
from pathlib import Path
from types import FunctionType
from typing import Dict, Iterable, Mapping, Any, Tuple, Optional


def load_functions_from_notebook(
    notebook_path: str,
    function_names: Iterable[str],
    injected_globals: Optional[Mapping[str, Any]] = None,
) -> Tuple[Dict[str, FunctionType], Dict[str, Any]]:
    """Load selected function defs from a Databricks-exported .py notebook.

    This avoids executing top-level notebook side effects (AWS calls, Spark jobs, etc.).
    """
    path = Path(notebook_path)
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(path))

    selected = []
    wanted = set(function_names)
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in wanted:
            selected.append(node)

    module = ast.Module(body=selected, type_ignores=[])
    ast.fix_missing_locations(module)

    namespace: Dict[str, Any] = {"__builtins__": __builtins__}
    if injected_globals:
        namespace.update(dict(injected_globals))

    exec(compile(module, filename=str(path), mode="exec"), namespace)
    functions = {name: namespace[name] for name in wanted}
    return functions, namespace


