from python_etl.basic_etl import *
from utils import testpilot_etl_boilerplate

import dateutil.parser
from pyspark.sql.types import *
from pyspark.sql import Row


class Request:
    def __option__(func):
        return lambda x: func(x) if x is not None else None

    int_type = (__option__(int), LongType())
    float_type = (__option__(float), DoubleType())

    field_types = {
        'num': int_type,
        'cached': float_type,
        'cdn': float_type,
        'time': int_type,
    }

    StructType = StructType([
        # For some reason, field_types needs to be sorted. Use 
        # PYTHONHASHSEED = 3201792604 to reproduce failure
        StructField(key, field_types[key][1], True) for key in sorted(field_types)
    ])

    def __init__(self, request_dict):
        args = {field: conversion(request_dict.get(field))
                for field, (conversion, sql_type) in Request.field_types.items()}
        self.Row = Row(**args)



def transform_pings(sqlContext, pings):
    def requests_to_rows(requests):
        out =  {k: Request(v).Row for k, v in requests.items()}
        return out

    RequestsType = MapType(StringType(), Request.StructType)

    return convert_pings(
        sqlContext,
        pings,
        DataFrameConfig([
            ("method", "payload/payload/method", None, StringType()),
            ("id", "payload/payload/id", None, StringType()),
            ("type", "payload/payload/type", None, StringType()),
            ("object", "payload/payload/object", None, StringType()),
            ("category", "payload/payload/category", None, StringType()),
            ("variant", "payload/payload/variant", None, StringType()),
            ("details", "payload/payload/details", None, StringType()),
            ("sentiment", "payload/payload/sentiment", None, IntegerType()),
            ("reason", "payload/payload/reason", None, StringType()),
            ("adBlocker", "payload/payload/adBlocker", None, BooleanType()),
            ("addons", "payload/payload/addons", None, ArrayType(StringType())),
            ("channel", "payload/payload/channel", None, StringType()),
            ("hostname", "payload/payload/hostname", None, StringType()),
            ("language", "payload/payload/language", None, StringType()),
            ("openTabs", "payload/payload/openTabs", None, IntegerType()),
            ("openWindows", "payload/payload/openWindows", None, IntegerType()),
            ("platform", "payload/payload/platform", None, StringType()),
            ("protocol", "payload/payload/protocol", None, StringType()),
            ("telemetryId", "payload/payload/telemetryId", None, StringType()),
            ("timerContentLoaded", "payload/payload/timerContentLoaded", None, LongType()),
            ("timerFirstInteraction", "payload/payload/timerFirstInteraction", None, LongType()),
            ("timerFirstPaint", "payload/payload/timerFirstPaint", None, LongType()),
            ("timerWindowLoad", "payload/payload/timerWindowLoad", None, LongType()),
            ("inner_timestamp", "payload/payload/timestamp", None, LongType()),
            ("fx_version", "payload/payload/fx_version", None, StringType()),
            ("creation_date", "creationDate", dateutil.parser.parse, TimestampType()),
            ("test", "payload/test", None, StringType()),
            ("variants", "payload/variants", None, StringType()),
            ("timestamp", "payload/timestamp", None, LongType()),
            ("version", "payload/version", None, StringType()),
            ("requests", "payload/payload/requests", requests_to_rows, RequestsType),
            ("disconnectRequests","payload/payload/disconnectRequests", None, LongType()),
            ("consoleErrors","payload/payload/consoleErrors", None, LongType()),
            ("e10sStatus","payload/payload/e10sStatus", None, LongType()),
            ("e10sProcessCount","payload/payload/e10sProcessCount", None, LongType()),
            ("trackingProtection","payload/payload/trackingProtection", None, BooleanType())
        ],
        lambda ping: ping['payload/test'] == 'pulse@mozilla.com')
    )

etl_job = testpilot_etl_boilerplate(
    transform_pings,
    's3://telemetry-parquet/testpilot/txp_pulse/v1'
)
