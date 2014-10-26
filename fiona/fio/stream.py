import json
import logging
import sys
import warnings

import click

import fiona
from fiona.fio.bounds import bounds
from fiona.fio.cli import obj_gen, generator, processor, streaming


@streaming.command('open', short_help="Stream GeoJSON objects read from stdin")
@generator
@click.pass_context
def open_stream(ctx):
    """Read lines of JSON text and for each, yield a GeoJSON object.

    Begins a pipeline of internally streamed GeoJSON objects.
    """
    verbosity = (ctx.obj and ctx.obj['verbosity']) or 2
    logger = logging.getLogger('fio')
    stdin = click.get_text_stream('stdin')
    try:
        source = obj_gen(stdin)
        for obj in source:
            yield obj
        sys.exit(0)
    except Exception:
        logger.exception("Failed. Exception caught")
        sys.exit(1)


@streaming.command('close', short_help="Write streamed GeoJSON objects to stdout")
@click.option('--seq', is_flag=True, default=False,
              help="Use RS as text separator instead of LF. "
                   "Experimental (default: no).")
@processor
@click.pass_context
def close_stream(stream, ctx, seq):
    """Write streamed GeoJSON objects to stdout.

    Closes a pipeline of internally streamed GeoJSON objects.
    """
    verbosity = (ctx.obj and ctx.obj['verbosity']) or 2
    logger = logging.getLogger('fio')
    try:
        for obj in stream:
            if seq:
                click.echo(u'\001e')
            click.echo(json.dumps(obj))
        sys.exit(0)
    except Exception:
        logger.exception("Failed. Exception caught")
        sys.exit(1)
