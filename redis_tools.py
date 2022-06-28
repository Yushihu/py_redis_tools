from itertools import islice
from typing import Generator

from redis.asyncio.client import Redis, Pipeline
from redis.exceptions import WatchError


class Pipe(object):
    def __init__(self, handler: Redis):
        self._handler = handler
        self._actions = []

    def __copy__(self):
        return self.__class__(self._handler)

    def do(self, func, *args, **kwargs):
        self._actions.append((func, args, kwargs))
        return self

    def execute(self):
        pipe = self._handler.pipeline()
        actions = self._actions

        return _flush_pipe(pipe, actions)


class Transaction(Pipe):
    def __init__(self, handler: Redis | Pipeline):
        super(Transaction, self).__init__(handler)
        self._watches = set()

    def watch(self, *watches):
        self._watches.update(watches)

    async def execute(self):
        pipe = self._handler.pipeline()
        actions = self._actions

        watch_pipe = self._handler.pipeline()
        watches = self._watches

        with watch_pipe:
            err_count = 0
            while True:
                try:
                    watch_pipe.watch(*watches)
                    watch_pipe.multi()
                    await _flush_pipe(pipe, actions, watch_pipe=watch_pipe)
                    await watch_pipe.execute()
                    break
                except WatchError:
                    err_count += 1
                    if err_count > 20:
                        raise RuntimeError()
                    continue


async def _flush_pipe(pipe, actions, **context):
    generators = []
    for func, args, kwargs in actions:
        ret = func(*args, **kwargs, **context, pipe=pipe)
        if not isinstance(ret, Generator):
            continue
        generators.append(ret)

    rest = []

    for gen in generators:
        start = len(pipe)
        next(gen)
        stop = len(pipe)
        rest.append((gen, start, stop))

    results = await pipe.execute()

    while rest:
        new_rest = []

        for gen, in_start, in_stop in rest:
            value = islice(results, in_start, in_stop)
            start = len(pipe)
            try:
                gen.send(value)
            except StopIteration:
                continue
            stop = len(pipe)
            new_rest.append((gen, start, stop))

        results = await pipe.execute()
        rest = new_rest
