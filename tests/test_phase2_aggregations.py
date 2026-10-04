"""
tests/test_phase2_aggregations.py
---------------------------------
اختبارات التجميعات والعروض المادية (الأقسام 2 و 3).
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from aggregations import AGGREGATIONS_REGISTRY, list_aggregations  # noqa: E402
from materialized_views import MATERIALIZED_VIEWS_REGISTRY, list_materialized_views  # noqa: E402


def test_minimum_five_aggregations_registered():
    assert len(AGGREGATIONS_REGISTRY) >= 5
    assert len(list_aggregations()) >= 5


def test_sales_by_city_pipeline_structure():
    info = AGGREGATIONS_REGISTRY["sales_by_city"]
    pipeline = info["build_pipeline"]({"limit": 10})
    stages = [list(stage.keys())[0] for stage in pipeline]
    assert "$match" in stages
    assert "$group" in stages
    assert "$sort" in stages
    assert "$limit" in stages


def test_top_products_pipeline_structure():
    info = AGGREGATIONS_REGISTRY["top_products"]
    pipeline = info["build_pipeline"]({"limit": 5})
    stages = [list(stage.keys())[0] for stage in pipeline]
    assert "$unwind" in stages
    assert "$group" in stages
    assert "$sort" in stages


def test_monthly_sales_trend_pipeline():
    info = AGGREGATIONS_REGISTRY["monthly_sales_trend"]
    pipeline = info["build_pipeline"]({})
    stages = [list(stage.keys())[0] for stage in pipeline]
    assert "$match" in stages
    assert "$group" in stages


def test_at_least_two_materialized_views():
    views = list_materialized_views()
    assert len(views) >= 2
    view_names = {v["name"] for v in views}
    assert "daily_sales_summary" in view_names
    assert "top_products_summary" in view_names
