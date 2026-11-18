"""Write the API's OpenAPI schema to a file, without a database.

Used by the frontend to generate TypeScript types (``npm run gen:api`` in
web/), so the client always matches the pydantic response models.

    uv run python -m app.api.export_openapi web/openapi.json
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from .main import create_app


def main() -> None:
    out = Path(sys.argv[1] if len(sys.argv) > 1 else "openapi.json")
    schema = create_app().openapi()   # building the schema doesn't open the database
    out.write_text(json.dumps(schema, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
