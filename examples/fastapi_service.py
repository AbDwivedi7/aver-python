"""FastAPI. The library imports nothing from FastAPI; this file shows the wiring.

Run it:

    pip install fastapi uvicorn
    uvicorn examples.fastapi_service:app --reload
"""

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from pydantic import BaseModel

from aver import AsyncAverClient

aver = AsyncAverClient(
    api_key=os.environ.get("AVER_API_KEY", "demo-key"),
    stream_id="consumer-pl",
    policy_version="credit-policy-v4.2",
    redact=["applicant.pan", "applicant.name", "bureau_report.*"],
)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    yield
    # Not strictly required — the atexit handler also drains — but an explicit
    # flush on shutdown makes the last few records deterministic.
    await aver.close(timeout=10.0)


app = FastAPI(lifespan=lifespan)


class Application(BaseModel):
    id: str
    applicant: dict
    income: float
    obligations: float
    requested_amount: float


@app.post("/underwrite")
async def underwrite(application: Application):
    bureau = await fetch_bureau(application.applicant["pan"])
    dti = application.obligations / max(application.income, 1)

    async with aver.decision(session_id=application.id) as d:
        # observe() is local bookkeeping — no I/O, nothing to await, and no
        # event-loop time spent on audit traffic.
        d.observe("application_form", application.model_dump())
        d.observe("cibil_report", bureau)
        d.observe("dti_computation", {"dti": dti, "threshold": 0.5})

        d.model(version="scorecard-v7", feature_set="fs-2026-03")

        if dti > 0.5:
            decision = {"type": "deny", "reason_code": "DTI_EXCEEDED"}
        else:
            decision = {"type": "approve", "amount": application.requested_amount}

        d.record_action(decision)

    return decision


@app.get("/health/aver")
async def aver_health():
    """Worth exposing: `dropped` above zero means records are missing.

    Safe to surface publicly: `last_error` carries only a status ("HTTP 400"),
    never the endpoint's response body, which could echo record contents.
    """
    return aver.stats()


async def fetch_bureau(pan: str) -> dict:
    return {"bureau_report": {"score": 742, "enquiries": 3}}
