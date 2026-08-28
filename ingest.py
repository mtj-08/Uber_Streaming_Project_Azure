from pyspark import pipelines as dp
from pyspark.sql.functions import col

# Event Hubs configuration
EH_NAMESPACE = spark.conf.get("connection_string.eh.namespace")
EH_NAME = spark.conf.get("connection_string.eh.name")


EH_CONN_STR = spark.conf.get("connection_string")

KAFKA_OPTIONS = {
    "kafka.bootstrap.servers":
        f"{EH_NAMESPACE}.servicebus.windows.net:9093",

    "subscribe": EH_NAME,

    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",

    "kafka.sasl.jaas.config":
        f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule '
        f'required username="$ConnectionString" password="{EH_CONN_STR}";',

    "kafka.request.timeout.ms": "60000",
    "kafka.session.timeout.ms": "30000",

    "startingOffsets": "earliest",
    "failOnDataLoss": "true",
    "maxOffsetsPerTrigger": "10000"
}


@dp.table
def rides_raw():

    df = (
        spark.readStream
            .format("kafka")
            .options(**KAFKA_OPTIONS)
            .load()
    )

    return df.withColumn(
        "rides",
        col("value").cast("string")
    )