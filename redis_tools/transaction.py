from enum import Enum, auto
from typing import Union

from redis import Redis
from redis.client import Pipeline
from redis.exceptions import WatchError


class Transaction:
    def __init__(self, client: Redis):
        self._pipe = client.pipeline()
        self._watches = set()
        self._read_funcs = []
        self._write_funcs = []
        self._end_funcs = []
        self._status = _Status.IDLE

    def reading(self, func):
        assert self._status is _Status.IDLE
        self._read_funcs.append(func)
        return func

    def writing(self, func):
        assert self._status.value < _Status.WRITING.value
        self._write_funcs.append(func)
        return func

    def ending(self, func):
        assert self._status.value < _Status.DONE.value
        self._end_funcs.append(func)
        return func

    def watch(self, *names):
        assert self._status is _Status.IDLE
        self._watches.update(names)

    def execute(self):
        assert self._status is _Status.IDLE
        initial_write_funcs = self._write_funcs.copy()
        initial_end_funcs = self._end_funcs.copy()

        err_count = 0
        with self._pipe as pipe:
            while True:
                # watch
                pipe.watch(*self._watches)

                # reading
                self._status = _Status.READING
                for func in self._read_funcs:
                    func()

                pipe.multi()

                # writing
                self._status = _Status.WRITING
                for func in self._write_funcs:
                    func()

                try:
                    pipe.execute()
                except WatchError:
                    if err_count > 10:
                        raise RuntimeError('Too many watch errors!')
                    # reset
                    self._write_funcs = initial_write_funcs.copy()
                    self._end_funcs = initial_end_funcs.copy()
                    continue

                self._status = _Status.DONE

                for func in self._end_funcs:
                    func()

                break

    @property
    def pipe(self) -> Union[Redis, Pipeline]:
        return self._pipe


class _Status(Enum):
    IDLE = auto()
    READING = auto()
    WRITING = auto()
    DONE = auto()
