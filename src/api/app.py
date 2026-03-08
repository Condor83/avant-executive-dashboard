"""FastAPI application factory and entry point."""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routers import alerts, data_quality, markets, meta, portfolio, summary, wallets


def create_app() -> FastAPI:
    """Build and configure the FastAPI application."""
    application = FastAPI(title="Avant Executive Dashboard API")

    application.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    application.include_router(summary.router)
    application.include_router(portfolio.router)
    application.include_router(wallets.router)
    application.include_router(markets.router)
    application.include_router(alerts.router)
    application.include_router(data_quality.router)
    application.include_router(meta.router)

    return application


app = create_app()
