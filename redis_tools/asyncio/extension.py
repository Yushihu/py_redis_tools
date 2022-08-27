from itertools import islice
from typing import Generator

from redis.exceptions import WatchError
from redis.asyncio.client import Redis, Pipeline

from ..extension import Extension as _Extension
from ..scripts import LUA_HSETXX, LUA_HPATCH


class Extension(_Extension):
    async def install_extension(self):
        await self.script_load(LUA_HSETXX)
        await self.script_load(LUA_HPATCH)


class _Common(object):
    def __init__(self, handler: Redis):
        self.handler = handler
        self._actions = []

    def __copy__(self):
        return self.__class__(self.handler)

    def do(self, func, *args, **kwargs):
        self._actions.append((func, args, kwargs))
        return self

    def execute(self):
        pipe = self.handler.pipeline()
        actions = self._actions

        return _flush_pipe(pipe, actions)


class Flow(_Common):
    pass


class Transaction(_Common):
    def __init__(self, handler: Redis | Pipeline):
        super(Transaction, self).__init__(handler)
        self._watches = set()

    def watch(self, *watches):
        self._watches.update(watches)

    async def execute(self):
        pipe = self.handler.pipeline()
        watch_pipe = self.handler.pipeline()
        watches = self._watches

        actions = [
            (func, (watch_pipe,) + args, kwargs)
            for func, args, kwargs in self._actions
        ]

        with watch_pipe:
            err_count = 0
            while True:
                try:
                    if watches:
                        watch_pipe.watch(*watches)
                        watch_pipe.multi()
                    await _flush_pipe(pipe, actions)
                    await watch_pipe.execute()
                    break
                except WatchError:
                    err_count += 1
                    if err_count > 20:
                        raise RuntimeError()
                    continue


async def _flush_pipe(pipe, actions):
    generators = []
    for func, args, kwargs in actions:
        ret = func(pipe, *args, **kwargs)
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
