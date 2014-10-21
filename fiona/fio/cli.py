from functools import update_wrapper
import json
import logging
import sys
import warnings

import click

from fiona import __version__ as fio_version


warnings.simplefilter('default')

def configure_logging(verbosity):
    log_level = max(10, 30 - 10*verbosity)
    logging.basicConfig(stream=sys.stderr, level=log_level)


def print_version(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    click.echo(fio_version)
    ctx.exit()


# The CLI command group.
@click.group(help="Fiona command line interface.")
@click.option('--verbose', '-v', count=True, help="Increase verbosity.")
@click.option('--quiet', '-q', count=True, help="Decrease verbosity.")
@click.option('--version', is_flag=True, callback=print_version,
              expose_value=False, is_eager=True,
              help="Print Fiona version.")
@click.pass_context
def cli(ctx, verbose, quiet):
    verbosity = verbose - quiet
    configure_logging(verbosity)
    ctx.obj = {}
    ctx.obj['verbosity'] = verbosity


def obj_gen(lines):
    """Return a generator of JSON objects loaded from ``lines``."""
    first_line = next(lines)
    if first_line.startswith(u'\x1e'):
        def gen():
            buffer = first_line.strip(u'\x1e')
            for line in lines:
                if line.startswith(u'\x1e'):
                    if buffer:
                        yield json.loads(buffer)
                    buffer = line.strip(u'\x1e')
                else:
                    buffer += line
            else:
                yield json.loads(buffer)
    else:
        def gen():
            yield json.loads(first_line)
            for line in lines:
                yield json.loads(line)
    return gen()


# The Streaming command group.
@click.group(chain=True, #invoke_without_command=True,
             help="Fiona's streaming commands.")
@click.option('--verbose', '-v', count=True, help="Increase verbosity.")
@click.option('--quiet', '-q', count=True, help="Decrease verbosity.")
@click.option('--version', is_flag=True, callback=print_version,
              expose_value=False, is_eager=True,
              help="Print Fiona version.")
@click.pass_context
def streaming(ctx, verbose, quiet):
    verbosity = verbose - quiet
    configure_logging(verbosity)
    ctx.obj = {}
    ctx.obj['verbosity'] = verbosity


@streaming.resultcallback()
def process_commands(processors, verbose, quiet):
    """This result callback is invoked with an iterable of all the chained
    subcommands.  As in this example each subcommand returns a function
    we can chain them together to feed one into the other, similar to how
    a pipe on unix works.
    """
    # Start with an empty iterable.
    stream = ()

    # Pipe it through all stream processors.
    for processor in processors:
        if processor.func_name == 'cat':
            continue
        stream = processor(stream)

    # Evaluate the stream and throw away the items.
    for _ in stream:
        pass


def processor(f):
    """Helper decorator to rewrite a function so that it returns another
    function from it.
    """
    def new_func(*args, **kwargs):
        def processor(stream):
            return f(stream, *args, **kwargs)
        return processor
    return update_wrapper(new_func, f)


def generator(f):
    """Similar to the :func:`processor` but passes through old values
    unchanged and does not pass through the values as parameter.
    """
    @processor
    def new_func(stream, *args, **kwargs):
        for item in stream:
            yield item
        for item in f(*args, **kwargs):
            yield item
    return update_wrapper(new_func, f)
