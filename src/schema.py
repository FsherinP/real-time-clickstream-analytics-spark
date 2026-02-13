from pyspark.sql.types import *

telemetry_schema = StructType([
    StructField("eid", StringType()),
    StructField("ets", LongType()),
    StructField("ver", StringType()),
    StructField("mid", StringType()),

    StructField("actor", StructType([
        StructField("id", StringType()),
        StructField("type", StringType())
    ])),

    StructField("context", StructType([
        StructField("channel", StringType()),
        StructField("env", StringType()),
        StructField("sid", StringType()),
        StructField("did", StringType()),
        StructField("pdata", StructType([
            StructField("id", StringType()),
            StructField("ver", StringType()),
            StructField("pid", StringType())
        ]))
    ])),

    StructField("object", StructType([
        StructField("id", StringType()),
        StructField("ver", StringType())
    ])),

    StructField("edata", StructType([
        StructField("pageid", StringType()),
        StructField("type", StringType()),
        StructField("uri", StringType()),
        StructField("duration", LongType()),
        StructField("subtype", LongType())
    ]))
])
