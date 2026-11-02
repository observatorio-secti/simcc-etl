from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse

from simcc_etl.core.database import create_etl_tables
from simcc_etl.core.logging import configure_logging
from simcc_etl.dashboard import dashboard_router
from simcc_etl.routers import router


@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging()
    try:
        create_etl_tables()
    except Exception:
        pass
    yield


app = FastAPI(
    title='SimCC ETL Service',
    description='Pipeline de ETL desacoplado do SIMCC',
    version='0.1.0',
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)

app.include_router(dashboard_router)
app.include_router(router)


@app.get('/', include_in_schema=False)
def root_redirect():
    return RedirectResponse(url='/dashboard')


@app.get('/health', tags=['Health'])
def health_check():
    return {'status': 'ok'}
