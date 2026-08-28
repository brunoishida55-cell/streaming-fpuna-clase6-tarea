from __future__ import annotations

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.test_stream import TestStream as BeamTestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import TimestampedValue


def test_teststream_accepts_late_event(solution):
    stream = (
        BeamTestStream()
        .advance_watermark_to(0)
        .add_elements(
            [
                TimestampedValue(("m1", 100), 5),
                TimestampedValue(("m1", 70), 40),
            ]
        )
        .advance_watermark_to(60)
        .add_elements(
            [
                TimestampedValue(("m1", 20), 50),
            ]
        )
        .advance_watermark_to_infinity()
    )

    options = PipelineOptions(streaming=True)

    with BeamTestPipeline(options=options) as pipeline:
        output = (
            pipeline
            | "Temporal input" >> stream
            | "Temporal policy"
            >> solution.build_trigger_policy(
                window_seconds=60,
                allowed_lateness_seconds=120,
            )
            | "Accumulating total" >> beam.CombinePerKey(sum)
        )

        assert_that(
            output,
            equal_to(
                [
                    ("m1", 170),
                    ("m1", 190),
                ]
            ),
            label="Late event accepted",
        )
