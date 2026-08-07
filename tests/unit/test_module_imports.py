"""Verify that all application modules import correctly."""


def test_producer_imports() -> None:
    import producer
    import producer.config
    import producer.connectors
    import producer.preparation

    assert producer is not None


def test_processing_imports() -> None:
    import processing
    import processing.config
    import processing.jobs
    import processing.transformations
    import processing.validation

    assert processing is not None


def test_warehouse_imports() -> None:
    import warehouse
    import warehouse.database
    import warehouse.migrations
    import warehouse.repositories
    import warehouse.schemas

    assert warehouse is not None


def test_analytics_imports() -> None:
    import analytics
    import analytics.dashboards
    import analytics.metrics
    import analytics.reports

    assert analytics is not None


def test_monitoring_imports() -> None:
    import monitoring
    import monitoring.grafana
    import monitoring.health
    import monitoring.logging
    import monitoring.metrics
    import monitoring.prometheus

    assert monitoring is not None
