"""FastAPI application entry point — R System Backend."""

import logging
import sys
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

# Ensure src/ is importable for the data providers
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from app.config import get_settings
from app.database import init_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

settings = get_settings()

app = FastAPI(
    title="老查理 — 深度价值投资顾问",
    version="0.2.0",
    description="老查理 — 深度价值投资分析系统后端",
)

# CORS — allow frontend dev server
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup():
    """Initialize DB tables on startup."""
    import app.models  # noqa: F401 — ensure all ORM models registered before create_all
    init_db()
    logger.info("Database tables created/verified.")
    llm_status = "configured" if settings.effective_api_key else "NOT configured"
    logger.info(f"LLM status: API_KEY {llm_status}")
    logger.info(f"Model: {settings.effective_model}")


# ── Mount API routers ──
from app.api.agent_routes import router as agent_router  # noqa: E402
from app.api.symbol_routes import router as symbol_router  # noqa: E402

app.include_router(agent_router, prefix="/api/v1")
app.include_router(symbol_router, prefix="/api")


# ── Serve frontend static files ──
_WEB_DIR = _PROJECT_ROOT / "web"
if _WEB_DIR.exists():
    app.mount("/assets", StaticFiles(directory=str(_WEB_DIR / "assets")), name="assets")

    @app.get("/", response_class=FileResponse)
    async def root():
        return FileResponse(str(_WEB_DIR / "index.html"))
else:
    @app.get("/")
    async def root():
        return {"name": "老查理", "version": "0.2.0", "status": "running"}


@app.get("/api/v1/health")
async def health():
    from app.agent.llm import is_llm_available, get_llm_model
    return {
        "status": "ok",
        "llmAvailable": is_llm_available(),
        "model": get_llm_model(),
    }
