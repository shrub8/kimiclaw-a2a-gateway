"""Contract: the spec served at /openapi.json equals the frozen openapi.yaml; new probes behave."""
import os
import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def test_served_spec_matches_frozen_yaml(gw_factory):
    served = gw_factory().get("/openapi.json").json()
    with open(os.path.join(ROOT, "openapi.yaml")) as f:
        assert served == yaml.safe_load(f)


def test_docs_still_served(gw_factory):
    assert "swagger" in gw_factory().get("/docs").text.lower()


def test_healthz_readyz(gw_factory):
    c = gw_factory()
    assert c.get("/healthz").json() == {"status": "ok"}
    assert c.get("/readyz").json() == {"status": "ready"}  # fake openclaw is on PATH
    assert c.get("/healthz", headers={"Authorization": ""}).status_code == 401


def test_readyz_without_openclaw(gw_factory):
    c = gw_factory(PATH="/usr/bin:/bin")
    r = c.get("/readyz")
    assert r.status_code == 503 and r.json()["status"] == "not ready"


def test_telemetry_files_written_without_message_text(gw_factory, tmp_path):
    tdir = tmp_path / "telemetry"
    c = gw_factory(GATEWAY_TELEMETRY_DIR=str(tdir))
    c.post("/", json={"jsonrpc": "2.0", "id": "1", "method": "message/send",
                      "params": {"message": {"parts": [{"kind": "text", "text": "SECRET-TEXT-42"}], "taskId": "tt"}}})
    logs = (tdir / "logs.jsonl").read_text()
    assert '"task_id": "tt"' in logs and '"status": 200' in logs and '"rpc_method": "message/send"' in logs
    assert "SECRET-TEXT-42" not in logs and "echo:" not in logs


def test_traces_and_metrics_files(tmp_path):
    import httpx
    from conftest import start_gateway, TOKEN
    tdir = tmp_path / "tel"
    p, base, _ = start_gateway(tmp_path, {"GATEWAY_TELEMETRY_DIR": str(tdir)})
    httpx.get(base + "/", headers={"Authorization": f"Bearer {TOKEN}"})
    p.terminate(); p.wait(10)
    traces = (tdir / "traces.jsonl").read_text()
    assert '"name": "rpc"' in traces
    assert "gateway.requests" in (tdir / "metrics.jsonl").read_text()
