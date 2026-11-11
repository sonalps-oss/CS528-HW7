
import argparse
import logging
import os
import time
from collections import Counter

import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.pvalue import TaggedOutput

from html_parser import (
    parse_html,
    extract_trigrams,
    resolve_link,
)


class ExtractMetrics(beam.DoFn):
    """Extract incoming links, outgoing links, and trigram counts."""

    def process(self, readable_file):
        filename = os.path.basename(readable_file.metadata.path)
        html = readable_file.read_utf8()

        links, visible_text = parse_html(html)

        # Count every hyperlink occurrence as an outgoing link.
        yield TaggedOutput(
            "outgoing",
            (filename, len(links))
        )

        # Each hyperlink contributes one incoming link.
        for href in links:
            target = resolve_link(filename, href)

            if target is not None:
                yield TaggedOutput(
                    "incoming",
                    (target, 1)
                )

        # Optimization: aggregate repeated trigrams per document.
        document_trigram_counts = Counter(
            " ".join(trigram)
            for trigram in extract_trigrams(visible_text)
        )

        for trigram, count in document_trigram_counts.items():
            yield TaggedOutput(
                "trigrams",
                (trigram, count)
            )


class TopFive(beam.CombineFn):
    """Return top five entries with deterministic tie-breaking."""

    def create_accumulator(self):
        return []

    def add_input(self, accumulator, item):
        accumulator.append(item)
        return self._trim(accumulator)

    def merge_accumulators(self, accumulators):
        merged = []

        for accumulator in accumulators:
            merged.extend(accumulator)

        return self._trim(merged)

    def extract_output(self, accumulator):
        return self._trim(accumulator)

    @staticmethod
    def _trim(items):
        return sorted(
            items,
            key=lambda item: (-item[1], item[0])
        )[:5]


def format_row(item):
    name, count = item
    return f"{name}\t{count}"


def write_top_five(pcollection, label, output_prefix):
    (
        pcollection
        | f"{label}: Top Five"
        >> beam.CombineGlobally(TopFive())
        | f"{label}: Expand Results"
        >> beam.FlatMap(lambda rows: rows)
        | f"{label}: Format Results"
        >> beam.Map(format_row)
        | f"{label}: Write Results"
        >> beam.io.WriteToText(
            f"{output_prefix}/{label}",
            file_name_suffix=".txt",
            num_shards=1,
        )
    )


def run(argv=None):
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input",
        required=True,
        help="Local HTML pattern or gs:// URI",
    )

    parser.add_argument(
        "--output",
        required=True,
        help="Output directory",
    )

    known_args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args)

    start_time = time.perf_counter()

    with beam.Pipeline(options=options) as pipeline:

        pages = (
            pipeline
            | "Match HTML Files"
            >> fileio.MatchFiles(
                known_args.input,
                empty_match_treatment=(
                    fileio.EmptyMatchTreatment.DISALLOW
                ),
            )
            | "Read HTML Files"
            >> fileio.ReadMatches()
        )

        metrics = (
            pages
            | "Extract HTML Metrics"
            >> beam.ParDo(ExtractMetrics()).with_outputs(
                "incoming",
                "outgoing",
                "trigrams",
            )
        )

        # Sum incoming references across all source files.
        incoming_counts = (
            metrics.incoming
            | "Sum Incoming Links"
            >> beam.CombinePerKey(sum)
        )

        # Outgoing links were already counted within each file.
        outgoing_counts = metrics.outgoing

        # Sum per-document trigram frequencies globally.
        trigram_counts = (
            metrics.trigrams
            | "Sum Trigram Frequencies"
            >> beam.CombinePerKey(sum)
        )

        write_top_five(
            incoming_counts,
            "incoming",
            known_args.output,
        )

        write_top_five(
            outgoing_counts,
            "outgoing",
            known_args.output,
        )

        write_top_five(
            trigram_counts,
            "trigrams",
            known_args.output,
        )

    elapsed = time.perf_counter() - start_time

    logging.info(
        "Pipeline execution call elapsed: %.3f seconds",
        elapsed,
    )

    print(
        f"Pipeline execution call elapsed: {elapsed:.3f} seconds"
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
