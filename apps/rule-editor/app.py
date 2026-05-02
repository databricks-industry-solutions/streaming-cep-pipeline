import os
import sys
import logging
import fastapi
import uvicorn
from fastapi.staticfiles import StaticFiles

from backend.api import router as rules_router

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = fastapi.FastAPI(title="CEP Rules Editor")

# /api/rules CRUD routes
app.include_router(rules_router)


@app.get("/health")
def health():
    return {"status": "ok", "python_version": sys.version}


# Serve the React build. Mounted last so /api/* and /health match first.
DIST_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "frontend", "dist")
if os.path.isdir(DIST_DIR):
    app.mount("/", StaticFiles(directory=DIST_DIR, html=True), name="frontend")
else:
    logger.warning(
        f"Frontend dist not found at {DIST_DIR}; UI will not be served. "
        f"Run `npm install && npm run build` in ./frontend first."
    )


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
