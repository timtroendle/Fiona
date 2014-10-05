from functools import partial
import json
import logging
import sys

import click
from shapely.geometry import mapping, shape

import fiona
from fiona.transform import transform_geom
from fiona.fio.cli import cli


# Buffer command
@cli.command(short_help="Buffer by a constant value a sequence of features.")
@click.argument('value', type=float)
@click.option('--ignore-errors/--no-ignore-errors', default=False,
              help="log errors but do not stop serialization.")
@click.option('--x-json-seq-rs/--x-json-seq-no-rs', default=True,
        help="Use RS as text separator instead of LF. Experimental.")
@click.pass_context
def buffer(ctx, value, ignore_errors, x_json_seq_rs):
    """Buffer by a constant value the geometries of a sequence of GeoJSON features
    and print the features."""
    verbosity = ctx.obj['verbosity']
    logger = logging.getLogger('fio')
    stdin = click.get_text_stream('stdin')
    sink = click.get_text_stream('stdout')

    first_line = next(stdin)
    # If input is RS-delimited JSON sequence.
    if first_line.startswith(u'\x1e'):
        def feature_gen():
            buffer = first_line.strip(u'\x1e')
            for line in stdin:
                if line.startswith(u'\x1e'):
                    if buffer:
                        yield json.loads(buffer)
                    buffer = line.strip(u'\x1e')
                else:
                    buffer += line
            else:
                yield json.loads(buffer)
    else:
        def feature_gen():
            yield json.loads(first_line)
            for line in stdin:
                yield json.loads(line)

    try:
        source = feature_gen()
        for feat in source:
            g = shape(feat['geometry']).buffer(value)
            feat['geometry'] = mapping(g)
            if x_json_seq_rs:
                sink.write(u'\u001e')
            json.dump(feat, sink)
            sink.write("\n")

        sys.exit(0)
    except Exception:
        logger.exception("Failed. Exception caught")
        sys.exit(1)
