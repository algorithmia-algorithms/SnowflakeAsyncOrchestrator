from . import SnowflakeAsyncOrchestrator

def test_SnowflakeAsyncOrchestrator():
    assert SnowflakeAsyncOrchestrator.apply("Jane") == "hello Jane"
