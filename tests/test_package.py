import medallion
from medallion import controller, databricks, ingestion


def test_package_exposes_version():
    assert medallion.__version__


def test_top_level_modules_are_importable():
    assert {controller.__name__, ingestion.__name__, databricks.__name__} == {
        "medallion.controller",
        "medallion.ingestion",
        "medallion.databricks",
    }
