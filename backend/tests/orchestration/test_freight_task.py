"""Integration-style entry point for debugging the freight calculation pipeline."""

import uuid
import pytest
from sqlalchemy import text

from app.core.config import settings
from app.db.session import SessionLocal
from app.db.model.freight import FreightRun
from app.orchestration.freight_calculation import freight_task


def _db_ready() -> bool:
    try:
        db = SessionLocal()
        db.execute(text("SELECT 1"))
        db.close()
        return True
    except Exception:
        return False


@pytest.mark.integration
def test_kick_freight_calc_full_flow():
    if not _db_ready():
        pytest.skip("Database connection is not available for freight task integration test")

    print("[debug] running kick_freight_calc with inline execution")
    result = freight_task.kick_freight_calc.run(product_run_id="81aaf56b-2fda-41ae-b83f-7d55588bb7ad", trigger="test-debug-1109-1")

    print("[debug] kick_freight_calc result", result)
    assert isinstance(result, dict)
    run_id = result.get("freight_run_id")
    assert run_id, "freight_run_id missing from result"

    db = SessionLocal()
    try:
        run = db.get(FreightRun, run_id)
        print("[debug] freight run record", run)
        assert run is not None, "FreightRun not created"
        print(
            "[debug] freight run state",
            {
                "id": run.id,
                "status": run.status,
                "candidate_count": getattr(run, "candidate_count", None),
                "changed_count": getattr(run, "changed_count", None),
                "message": getattr(run, "message", None),
            },
        )
    finally:
        db.close()
