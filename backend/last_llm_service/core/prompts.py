import json
from pathlib import Path

PROMPTS_DIR = Path(__file__).resolve().parent.parent / "prompts"

def _load(path: Path) -> str:
    """A prompt file is a JSON array of lines, kept as an array so edits diff cleanly."""
    lines = json.loads(path.read_text())
    return "\n".join(lines) if isinstance(lines, list) else str(lines)

# One prompt per json file, keyed by filename stem
PROMPTS: dict[str, str] = {p.stem: _load(p) for p in sorted(PROMPTS_DIR.glob("*.json"))}
