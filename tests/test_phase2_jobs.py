"""
tests/test_phase2_jobs.py
-------------------------
اختبارات المهام المجدولة والتشغيل اليدوي وتسجيل النتائج (القسم 4).
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from scheduler import JOBS_METADATA, list_jobs_status, run_job_now  # noqa: E402


def test_at_least_two_scheduled_jobs():
    assert len(JOBS_METADATA) >= 2
    assert "refresh_materialized_views" in JOBS_METADATA
    assert "generate_periodic_report" in JOBS_METADATA


def test_job_metadata_structure():
    for name, info in JOBS_METADATA.items():
        assert "title" in info
        assert "description" in info
        assert "interval_minutes" in info
        assert info["interval_minutes"] > 0
        assert callable(info["action"])


def test_list_jobs_status():
    jobs = list_jobs_status()
    assert len(jobs) >= 2
    names = {j["job_name"] for j in jobs}
    assert "refresh_materialized_views" in names


def test_manual_run_records_log_correctly():
    # تجربة استدعاء دالة التشغيل وتسجيل البداية والنهاية
    result = run_job_now("generate_periodic_report", trigger_mode="test_manual")
    assert result["job_name"] == "generate_periodic_report"
    assert result["trigger"] == "test_manual"
    assert "started_at" in result
    assert "ended_at" in result
    assert "duration_seconds" in result
    assert result["status"] in ("SUCCESS", "FAILED")
