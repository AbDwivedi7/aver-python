"""Redaction.

The claim this library makes to a security reviewer is: marked fields never
leave the process in the clear, and your objects are never touched. These
tests are that claim.
"""

from __future__ import annotations

import copy
import json
import logging
import os
import time

import pytest

from aver import AverClient, decrypt_field
from aver.errors import AverConfigError
from aver.redact import MASK, Redactor
from conftest import FakeTransport


def bureau_payload():
    return {
        "applicant": {"pan": "ABCDE1234F", "name": "Asha Rao", "age": 34},
        "bureau_report": {"score": 742, "enquiries": 3, "vintage_months": 96},
        "accounts": [
            {"id": "a1", "balance": 125000, "type": "credit_card"},
            {"id": "a2", "balance": 40000, "type": "auto_loan"},
        ],
        "dti": 0.61,
    }


class TestPathMatching:
    def test_nested_field(self):
        out = Redactor(["applicant.pan"]).apply(bureau_payload())
        assert out["applicant"]["pan"] == MASK
        assert out["applicant"]["name"] == "Asha Rao"  # not marked, not touched

    def test_wildcard_one_level_under(self):
        out = Redactor(["bureau_report.*"]).apply(bureau_payload())
        assert set(out["bureau_report"].values()) == {MASK}
        assert out["dti"] == 0.61  # the wildcard does not escape its subtree

    def test_array_element_field(self):
        out = Redactor(["accounts[].balance"]).apply(bureau_payload())
        assert [a["balance"] for a in out["accounts"]] == [MASK, MASK]
        assert [a["id"] for a in out["accounts"]] == ["a1", "a2"]

    def test_multiple_paths_together(self):
        out = Redactor(
            ["applicant.pan", "applicant.name", "bureau_report.*", "accounts[].balance"]
        ).apply(bureau_payload())
        assert out["applicant"] == {"pan": MASK, "name": MASK, "age": 34}
        assert out["accounts"][1]["balance"] == MASK

    def test_top_level_array(self):
        """An input whose value is itself a list of accounts."""
        accounts = [{"id": "a1", "balance": 1000}, {"id": "a2", "balance": 2000}]
        out = Redactor(["[].balance"]).apply(accounts)
        assert [a["balance"] for a in out] == [MASK, MASK]
        assert [a["id"] for a in out] == ["a1", "a2"]

    def test_top_level_array_nested_field(self):
        rows = [{"meta": {"pan": "ABCDE1234F", "ok": True}}]
        out = Redactor(["[].meta.pan"]).apply(rows)
        assert out[0]["meta"]["pan"] == MASK
        assert out[0]["meta"]["ok"] is True

    def test_top_level_array_path_against_a_dict_is_a_no_op(self):
        out = Redactor(["[].balance"]).apply(bureau_payload())
        assert out == bureau_payload()

    def test_double_dot_is_still_rejected(self):
        """The leading-[] allowance must not let a real typo through."""
        with pytest.raises(AverConfigError):
            Redactor(["accounts.[].balance"])

    def test_missing_path_is_not_an_error(self):
        out = Redactor(["nope.not_here", "applicant.aadhaar"]).apply(bureau_payload())
        assert out == bureau_payload()

    def test_path_pointing_at_a_scalar_stops_cleanly(self):
        out = Redactor(["dti.something"]).apply(bureau_payload())
        assert out["dti"] == 0.61

    def test_non_dict_input_survives(self):
        assert Redactor(["a.b"]).apply(["not", "a", "dict"]) == ["not", "a", "dict"]
        assert Redactor(["a.b"]).apply(None) is None

    def test_empty_path_rejected(self):
        with pytest.raises(AverConfigError):
            Redactor(["applicant..pan"])


class TestNoMutation:
    def test_callers_object_is_untouched(self):
        original = bureau_payload()
        reference = copy.deepcopy(original)
        Redactor(["applicant.pan", "bureau_report.*", "accounts[].balance"]).apply(
            original
        )
        assert original == reference

    def test_action_snapshot_survives_later_caller_mutation(self):
        """The action is what we claim they decided. It must not drift.

        Delivery is asynchronous, so holding the caller's dict by reference
        would make the recorded action depend on whether the flusher
        serialised before or after their next mutation.
        """
        transport = FakeTransport()
        client = AverClient(
            api_key="k", stream_id="s", transport=transport, flush_interval=3.0
        )
        action = {"type": "approve", "amount": 100}
        try:
            with client.decision(session_id="app-1") as d:
                d.record_action(action)
            action["type"] = "deny"  # caller reuses the dict
            action["amount"] = 999999
            assert client.flush(timeout=5.0)
            assert transport.delivered_records()[0]["action"] == {
                "type": "approve",
                "amount": 100,
            }
        finally:
            client.close(timeout=0.5)

    def test_snapshot_survives_later_caller_mutation(self):
        """The caller keeps using that dict after observe(). We took a copy."""
        transport = FakeTransport()
        client = AverClient(
            api_key="k", stream_id="s", transport=transport, flush_interval=0.02
        )
        form = {"applicant": {"pan": "ABCDE1234F", "amount": 100}}
        try:
            with client.decision(session_id="app-1") as d:
                d.observe("form", form)
                d.record_action({"type": "approve"})
            form["applicant"]["amount"] = 999999  # caller carries on
            assert client.flush(timeout=2.0)
            sent = transport.delivered_records()[0]["inputs"][0]["value"]
            assert sent["applicant"]["amount"] == 100
        finally:
            client.close(timeout=0.5)


class TestNothingLeaksOnTheWire:
    def test_marked_values_never_appear_in_any_payload(self):
        transport = FakeTransport()
        client = AverClient(
            api_key="k",
            stream_id="s",
            redact=["applicant.pan", "applicant.name", "bureau_report.*",
                    "accounts[].balance"],
            transport=transport,
            flush_interval=0.02,
        )
        try:
            with client.decision(session_id="app-1") as d:
                d.observe("cibil", bureau_payload())
                d.record_action({"type": "deny", "reason_code": "DTI_EXCEEDED"})
            assert client.flush(timeout=2.0)
            attempts = transport.attempts
            assert attempts, "nothing was ever put on the wire"

            # Structural first: exactly which fields were masked, on every
            # attempt (a retry must not ship an unredacted copy).
            for attempt in attempts:
                value = attempt.records[0]["inputs"][0]["value"]
                assert value["applicant"] == {"pan": MASK, "name": MASK, "age": 34}
                assert set(value["bureau_report"].values()) == {MASK}
                assert [a["balance"] for a in value["accounts"]] == [MASK, MASK]
                assert value["dti"] == 0.61  # unmarked, not over-redacted
                assert value["accounts"][0]["id"] == "a1"

            # Then a substring sweep, scoped to the inputs. Scanning the whole
            # record would also scan UUIDs and timestamps, where a chance "742"
            # reads as a PII leak and sends you hunting a bug that isn't there.
            inputs_wire = json.dumps([a.records[0]["inputs"] for a in attempts])
            for secret in ("ABCDE1234F", "Asha Rao", "742", "125000", "40000"):
                assert secret not in inputs_wire, "{0} reached the wire".format(
                    secret
                )
            assert "DTI_EXCEEDED" in json.dumps(
                [a.records[0]["action"] for a in attempts]
            )
        finally:
            client.close(timeout=0.5)


class TestUnmatchedPathWarning:
    def test_typo_warns_once(self, caplog):
        transport = FakeTransport()
        client = AverClient(
            api_key="k",
            stream_id="s",
            redact=["applicant.pan", "applicant.pan_number"],  # second is a typo
            transport=transport,
            flush_interval=0.02,
        )
        try:
            with caplog.at_level(logging.WARNING, logger="aver"):
                for _ in range(3):
                    client.record(
                        session_id="app",
                        inputs=[{"role": "form", "value": bureau_payload()}],
                        action={"type": "approve"},
                    )
            warnings = [r for r in caplog.records if "matched nothing" in r.message]
            assert len(warnings) == 1
            assert "applicant.pan_number" in caplog.text
            assert "applicant.pan," not in caplog.text  # the good path is not named
        finally:
            client.close(timeout=0.5)

    def test_no_warning_when_everything_matches(self, caplog):
        transport = FakeTransport()
        client = AverClient(
            api_key="k",
            stream_id="s",
            redact=["applicant.pan"],
            transport=transport,
            flush_interval=0.02,
        )
        try:
            with caplog.at_level(logging.WARNING, logger="aver"):
                client.record(
                    session_id="app",
                    inputs=[{"role": "form", "value": bureau_payload()}],
                    action={"type": "approve"},
                )
            assert "matched nothing" not in caplog.text
        finally:
            client.close(timeout=0.5)


class TestLargeInputProbe:
    def test_size_is_measured_exactly_once_per_client(self, monkeypatch):
        """Measuring costs a full serialisation, ~40% on top of the copy.

        Once per process is nothing; once per record is a tax on every
        decision. The probe must switch itself off whatever it finds — an
        input under the limit used to leave it armed forever.
        """
        calls = []

        class JsonSpy:
            """Counts dumps(), delegates everything else to the real module.

            Delegation rather than a bare stub: redact.py also reaches for
            json.loads (decrypt_field) and json.dumps (_encryptor). Stubbing
            only dumps would make any future json call on the mask path fail
            with an AttributeError instead of a useful assertion.
            """

            def dumps(self, *args, **kwargs):
                calls.append(1)
                return json.dumps(*args, **kwargs)

            def __getattr__(self, name):
                return getattr(json, name)

        monkeypatch.setattr("aver.redact.json", JsonSpy())

        small = {"applicant": {"pan": "ABCDE1234F"}}  # nowhere near the limit
        redactor = Redactor(["applicant.pan"])
        for _ in range(5):
            redactor.redact_inputs([{"role": "form", "value": small}])

        assert len(calls) == 1, "measured {0} times, expected 1".format(len(calls))
        assert redactor._probed_size is True

    def test_oversized_input_warns_once(self, caplog):
        chunk = "y" * 2000
        payload = {"rows": [{"i": i, "blob": chunk} for i in range(700)]}
        assert len(json.dumps(payload)) > 1_000_000
        redactor = Redactor([])
        with caplog.at_level(logging.WARNING, logger="aver"):
            for _ in range(3):
                redactor.redact_inputs([{"role": "cibil", "value": payload}])
        warnings = [r for r in caplog.records if "large inputs slow" in r.message]
        assert len(warnings) == 1
        assert "cibil" in warnings[0].getMessage()

    def test_big_but_cheap_to_copy_payload_is_still_caught(self, caplog):
        """The case a timing-based gate cannot see.

        ``deepcopy`` treats strings as atomic, so a payload whose bulk is long
        text — a bureau report with big free-text fields — is megabytes on the
        wire and almost free to copy. Measuring size catches it; timing the
        copy does not.
        """
        shared = "z" * 2000  # one string object, referenced 700 times
        payload = {"rows": [{"i": i, "blob": shared} for i in range(700)]}
        assert len(json.dumps(payload)) > 1_000_000

        before = time.perf_counter()
        copy.deepcopy(payload)
        assert (time.perf_counter() - before) < 0.005, "not actually cheap to copy"

        with caplog.at_level(logging.WARNING, logger="aver"):
            Redactor([]).redact_inputs([{"role": "cibil", "value": payload}])
        assert "large inputs slow" in caplog.text


class TestMissingRole:
    def test_input_without_a_role_still_records(self):
        """A direct record() call may omit 'role'; that must not lose the record."""
        transport = FakeTransport()
        client = AverClient(
            api_key="k", stream_id="s", transport=transport, flush_interval=0.02
        )
        try:
            result = client.record(
                session_id="app-1",
                inputs=[{"value": {"amount": 5}}],  # no "role"
                action={"type": "approve"},
            )
            assert result is not None
            assert client.flush(timeout=2.0)
            record = transport.delivered_records()[0]
            assert record["inputs"][0]["role"] == "?"
            assert record["inputs"][0]["value"] == {"amount": 5}
            assert client.stats()["dropped"] == 0
        finally:
            client.close(timeout=0.5)


class TestEncryptMode:
    def test_round_trip(self):
        key = os.urandom(32)
        out = Redactor(["applicant.pan"], mode="encrypt", key=key).apply(
            bureau_payload()
        )
        field = out["applicant"]["pan"]
        assert field["__aver__"] == "enc:aes-256-gcm"
        assert "ABCDE1234F" not in str(field)
        assert decrypt_field(field, key) == "ABCDE1234F"

    def test_ciphertext_is_not_deterministic(self):
        key = os.urandom(32)
        r = Redactor(["applicant.pan"], mode="encrypt", key=key)
        a = r.apply(bureau_payload())["applicant"]["pan"]["ciphertext"]
        b = r.apply(bureau_payload())["applicant"]["pan"]["ciphertext"]
        assert a != b  # fresh nonce per field

    def test_wrong_key_cannot_read_it(self):
        out = Redactor(["applicant.pan"], mode="encrypt", key=os.urandom(32)).apply(
            bureau_payload()
        )
        with pytest.raises(Exception):
            decrypt_field(out["applicant"]["pan"], os.urandom(32))

    def test_structured_values_round_trip(self):
        key = os.urandom(32)
        out = Redactor(["bureau_report"], mode="encrypt", key=key).apply(
            bureau_payload()
        )
        assert decrypt_field(out["bureau_report"], key) == {
            "score": 742,
            "enquiries": 3,
            "vintage_months": 96,
        }

    @pytest.mark.parametrize(
        "key", [None, b"too-short", "not base64!!", os.urandom(16)]
    )
    def test_bad_key_rejected_at_construction(self, key):
        with pytest.raises(AverConfigError):
            Redactor(["applicant.pan"], mode="encrypt", key=key)

    def test_base64_key_accepted(self):
        import base64

        raw = os.urandom(32)
        encoded = base64.b64encode(raw).decode("ascii")  # what sits in their config
        r = Redactor(["applicant.pan"], mode="encrypt", key=encoded)
        out = r.apply(bureau_payload())
        assert decrypt_field(out["applicant"]["pan"], raw) == "ABCDE1234F"

    def test_unknown_mode_rejected(self):
        with pytest.raises(AverConfigError):
            Redactor([], mode="hash")
