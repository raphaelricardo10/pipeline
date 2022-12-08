import os
from dotenv import load_dotenv
import argparse
from datetime import datetime
import logging
import json
import pandas as pd

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.transforms.window as window


load_dotenv()

WINDOW_SIZE=1
PROJECT_ID="studious-matrix-355222"

TOPIC_ID="sensor_data"
PUB_PATH=f"projects/{PROJECT_ID}/topics/{TOPIC_ID}"

BQ_DATASET="environmental_sensor_telemetry"
BQ_TABLE="multisensor_data"
BQ_TABLE_SPEC=f"{PROJECT_ID}:{BQ_DATASET}.{BQ_TABLE}"

class ResampleGroup(beam.DoFn):
    def process(self, element, *args, **kwargs):
        _, rows = element

        df = pd.DataFrame(rows)

        categorical_columns = ["device", "motion", "light"]
        df = df.groupby(categorical_columns).resample("1S", on="ts").mean(
            numeric_only=True
        ).dropna()
        
        df = df.drop(
            [x for x in categorical_columns if x in df.columns], axis=1
        ).reset_index()

        yield df.to_dict("records")

class RenameFieldsToBigQuery(beam.DoFn):
    def process(self, element, *args, **kwargs):
        element['timestamp'] = element.pop('ts')

        yield element


class ConvertDateTime(beam.DoFn):
    def process(self, element, *args, **kwargs):
        element["ts"] = datetime.fromtimestamp(element["ts"])

        yield element


def run(
    pipeline_args=None,
):
    print(pipeline_args)
    # Set `save_main_session` to True so DoFns can access globally imported modules.
    pipeline_options = PipelineOptions(
        streaming=True, save_main_session=True, flags=pipeline_args
    )

    with beam.Pipeline(options=pipeline_options) as pipeline:
        messages = (
            pipeline
            # Because `timestamp_attribute` is unspecified in `ReadFromPubSub`, Beam
            # binds the publish time returned by the Pub/Sub server for each message
            # to the element's timestamp parameter, accessible via `DoFn.TimestampParam`.
            # https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.pubsub.html#apache_beam.io.gcp.pubsub.ReadFromPubSub
            | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(topic=PUB_PATH)
            | "Decode elements" >> beam.Map(lambda msg: msg.decode("utf-8"))
            | "Parse JSON messages" >> beam.Map(lambda msg: json.loads(msg))
            | "Fixed-size windows" >> beam.WindowInto(window.FixedWindows(WINDOW_SIZE, 0))
            | "Convert to datetime" >> beam.ParDo(ConvertDateTime())
            | "Add device keys" >> beam.WithKeys(lambda msg: msg["device"])
            | "Group by device" >> beam.GroupByKey()
            | "Resample" >> beam.ParDo(ResampleGroup())
            | "Recombine the groups" >> beam.FlatMap(lambda x: x)
            # | "Match BQ field names" >> beam.ParDo(RenameFieldsToBigQuery())
            | "Write to Big Query" >> beam.io.WriteToBigQuery(BQ_TABLE_SPEC)
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    # parser.add_argument(
    #     "--input_topic",
    #     default=os.getenv("PUB_PATH"),
    #     help="The Cloud Pub/Sub topic to read from."
    #     '"projects/<PROJECT_ID>/topics/<TOPIC_ID>".',
    # )
    # parser.add_argument(
    #     "--window_size",
    #     type=float,
    #     default=1.0,
    #     help="Output file's window size in minutes.",
    # )
    # parser.add_argument(
    #     "--output_path",
    #     default=os.getenv("GS_PATH"),
    #     help="Path of the output GCS file including the prefix.",
    # )
    # parser.add_argument(
    #     "--num_shards",
    #     type=int,
    #     default=5,
    #     help="Number of shards to use when writing windowed elements to GCS.",
    # )
    # parser.add_argument(
    #     "--table_spec",
    #     default=os.getenv("BQ_TABLE_SPEC"),
    #     help="Big Query destination table",
    # )
    known_args, pipeline_args = parser.parse_known_args()

    run(
        # known_args.input_topic,
        # known_args.table_spec,
        # known_args.window_size,
        pipeline_args,
    )
