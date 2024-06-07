from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment, ProcessWindowFunction
from pyflink.datastream.window import TimeWindow, TumblingProcessingTimeWindows, SlidingProcessingTimeWindows
from pyflink.common.time import Time
from pyflink.common import Configuration, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaRecordSerializationSchema, KafkaSink, KafkaSource, KafkaSourceBuilder, KafkaOffsetsInitializer

from datetime import datetime, timedelta
import re

SECONDARY_SOURCE = 'i483-allsensors'
SECONDARY_SINK = 'i483-fvtt'

config = Configuration()
config.set_integer('parallelism.default', 1)
config.set_integer('taskmanager.numberOfTaskSlots', 2)

env = StreamExecutionEnvironment.get_execution_environment(config)
env.add_jars('file:///Users/nagutabby/flink-1.18.1/opt/flink-sql-connector-kafka-3.1.0-1.18.jar')

class CustomProcessWindowFunction(ProcessWindowFunction[tuple, tuple, str, TimeWindow]):
    is_room_occupied = False
    def process(self, key, context, elements):
        dict_primary_sources = {}
        for key, message in elements:
            primary_source, _, value = message.split(',')
            value = float(value)
            if primary_source not in dict_primary_sources:
                dict_primary_sources[primary_source] = [value]
            else:
                dict_primary_sources[primary_source].append(value)

        is_data_threshold_crossed = False

        for primary_source, values in dict_primary_sources.items():
            if primary_source == 'i483-sensors-s2410064-BMP180-temperature' or primary_source == 'i483-sensors-s2410064-SCD41-co2' or primary_source == 'i483-sensors-team2-BH1750-illumination':
                average_value = round(sum(values) / len(values), 1)
                current_value = values[-1]
                lower_limit = average_value * 0.8
                upper_limit = average_value * 1.2
                if not lower_limit < current_value < upper_limit:
                    is_data_threshold_crossed = True
                    break

        if is_data_threshold_crossed:
            self.is_room_occupied = not self.is_room_occupied
            if self.is_room_occupied:
                sink_message = 'i483-sensors-s2410064-is_room_occupied,true'
            else:
                sink_message = 'i483-sensors-s2410064-is_room_occupied,false'

            return [sink_message]
        else:
            return []

def create_data_stream_and_sink_back(secondary_source):
    unix_timestamp_five_minutes_ago_s = datetime.timestamp(datetime.now() - timedelta(minutes=10))
    unix_timestamp_five_minutes_ago_ms = round(unix_timestamp_five_minutes_ago_s * 1000)

    kafka_source = KafkaSourceBuilder() \
        .set_starting_offsets(KafkaOffsetsInitializer.timestamp(unix_timestamp_five_minutes_ago_ms)) \
        .set_topics(secondary_source) \
        .set_bootstrap_servers('150.65.230.59:9092') \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    data_stream = env.from_source(
        kafka_source,
        WatermarkStrategy.for_monotonous_timestamps(),
        'Kafka source'
    ) \
    .map(lambda x: (0, x), Types.TUPLE([Types.INT(), Types.STRING()])) \
    .key_by(lambda x: x[0]) \
    .window(SlidingProcessingTimeWindows.of(Time.minutes(10), Time.seconds(30))) \

    kafka_producer = FlinkKafkaProducer(
        SECONDARY_SINK,
        SimpleStringSchema(),
        {'bootstrap.servers': '150.65.230.59:9092'},
    )

    data_stream.process(CustomProcessWindowFunction(), Types.STRING()).add_sink(kafka_producer)

create_data_stream_and_sink_back(SECONDARY_SOURCE)

env.execute()
