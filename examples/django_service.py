"""Django. The library imports nothing from Django; this file shows the wiring.

Put the client in a module of its own (``lending/audit.py``) so you get one
per process rather than one per request. Threads share it safely.

    # settings.py
    AVER_API_KEY = env("AVER_API_KEY")

    # lending/audit.py
    from aver import AverClient
    from django.conf import settings

    aver = AverClient(
        api_key=settings.AVER_API_KEY,
        stream_id="consumer-pl",
        policy_version=settings.CREDIT_POLICY_VERSION,
        redact=["applicant.pan", "applicant.aadhaar", "bureau_report.*"],
    )

Gunicorn and uWSGI fork worker processes, so each worker builds its own client
and its own background thread. That is what you want: no shared socket across
a fork.
"""

from django.http import JsonResponse
from django.views.decorators.http import require_POST

# from lending.audit import aver
from aver import AverClient

aver = AverClient(
    api_key="demo-key",
    stream_id="consumer-pl",
    policy_version="credit-policy-v4.2",
    redact=["applicant.pan", "applicant.aadhaar", "bureau_report.*"],
)


@require_POST
def underwrite(request):
    application = _parse(request)
    bureau = fetch_bureau(application["applicant"]["pan"])

    with aver.decision(session_id=application["id"]) as d:
        d.observe("application_form", application)
        d.observe("cibil_report", bureau)

        d.model(version="scorecard-v7", feature_set="fs-2026-03")

        decision = run_scorecard(application, bureau)
        d.record_action(decision)

    # If run_scorecard raises, the record is still queued with
    # action={"type": "error", ...} and the exception propagates to Django's
    # handler unchanged — a decision that crashed still needs explaining. If
    # record_action() had already run, that action is kept under
    # action["attempted_action"]: "denied, then the write failed" is exactly
    # the case a reviewer asks about.
    return JsonResponse(decision)


# --- stand-ins for your real code -------------------------------------------


def _parse(request):
    raise NotImplementedError


def fetch_bureau(pan):
    raise NotImplementedError


def run_scorecard(application, bureau):
    raise NotImplementedError
