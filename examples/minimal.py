"""The smallest useful integration.

Run against a local stub:

    python -m http.server 8080 &
    AVER_BASE_URL=http://localhost:8080 python examples/minimal.py
"""

import os

from aver import AverClient

aver = AverClient(
    api_key=os.environ.get("AVER_API_KEY", "demo-key"),
    stream_id="consumer-pl",
    policy_version="credit-policy-v4.2",
    redact=["applicant.pan", "applicant.name", "bureau_report.*"],
    base_url=os.environ.get("AVER_BASE_URL", "https://api.aver.dev"),
)


def decide(application_id, form_data, bureau_response):
    """Your existing decision logic, with three lines added."""
    dti = form_data["obligations"] / max(form_data["income"], 1)

    with aver.decision(session_id=application_id) as d:
        d.observe("application_form", form_data)
        d.observe("cibil_report", bureau_response)
        d.observe("dti_computation", {"dti": dti, "threshold": 0.5})

        d.model(
            version="scorecard-v7",
            artifact_hash="sha256:9f2b7c1e",
            feature_set="fs-2026-03",
            rule_config_hash="sha256:44ab0d92",
        )

        if dti > 0.5:
            action = {"type": "deny", "reason_code": "DTI_EXCEEDED"}
        else:
            action = {"type": "approve", "amount": form_data["requested_amount"]}

        d.record_action(action)

    return action


if __name__ == "__main__":
    result = decide(
        application_id="APP-10021",
        form_data={
            "applicant": {"pan": "ABCDE1234F", "name": "Asha Rao", "age": 34},
            "income": 90000,
            "obligations": 55000,
            "requested_amount": 450000,
        },
        bureau_response={"bureau_report": {"score": 742, "enquiries": 3}},
    )
    print("decision:", result)

    # Batch jobs and scripts should drain explicitly; long-running services
    # do not need to — the atexit handler covers process shutdown.
    aver.flush(timeout=5.0)
    print("stats:", aver.stats())
