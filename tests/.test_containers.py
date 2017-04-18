import pytest
import json
from pyspark.sql import SQLContext, Row
from python_etl.testpilot.containers import transform_pings
from utils import *


def create_ping(payload):
    return {'payload': {
        'test': '@testpilot-containers',
        'other-ignored-field': 'who cares',
        'payload': payload
    }}


def create_row(overrides):
    keys = ["uuid", "userContextId", "clickedContainerTabCount", "eventSource",
            "event", "hiddenContainersCount", "shownContainersCount",
            "totalContainersCount", "totalContainerTabsCount",
            "totalNonContainerTabsCount", "test"]
    
    return Row(**{key: overrides.get(key, None) for key in keys})

example_payloads = [
    # Open a container ping
    {
        'uuid': 'a',
        'userContextId': 10,
        'clickedContainerTabCount': 20,
        'event': 'open-tab',
        'eventSource': 'tab-bar'
    },
    # Edit containers ping
    {
        'uuid': 'b',
        'event': 'edit-containers'
    },
    # Hide container tabs ping
    {
        'uuid': 'a',
        'userContextId': 'firefox-default',
        'clickedContainerTabCount': 5,
        'event': "hide-tabs",
        'hiddenContainersCount': 2,
        'shownContainersCount': 3,
        'totalContainersCount': 5,
    }
]

@pytest.fixture
def simple_ping_rdd(spark_context):
    # See the following document for a description of the possible ping types:
    # https://github.com/mozilla/testpilot-containers/blob/master/docs/metrics.md
    return spark_context.parallelize(map(create_ping, example_payloads))

# Tests
def test_simple_transform(simple_ping_rdd, spark_context):
    actual = transform_pings(SQLContext(spark_context), simple_ping_rdd).collect()
    expected = map(create_row, example_payloads)

    assert all(map(lambda key: actual[key] == expected[key], expected.keys()))

