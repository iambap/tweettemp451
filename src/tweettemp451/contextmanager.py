import contextlib


@contextlib.asynccontextmanager
async def aclosing(thing):
    """Like contextlib.closing, but async"""
    try:
        yield thing
    finally:
        await thing.aclose()
