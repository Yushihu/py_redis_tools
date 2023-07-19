from enum import Enum, auto
from typing import Union

from redis import Redis
from redis.client import Pipeline
from redis.exceptions import WatchError


class Transaction:
    def __init__(self, client: Redis, max_conflict=1000):
        self._pipe = client.pipeline()
        self._watches = set()
        self._read_funcs = []
        self._write_funcs = []
        self._end_funcs = []
        self._status = _Status.IDLE
        self._max_conflict = max_conflict

    def reading(self, func):
        assert self._status.value <= _Status.READING.value
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
        if self._status is _Status.IDLE:
            self._watches.update(names)
        elif self._status is _Status.READING:
            self._pipe.watch(*names)
        else:
            raise RuntimeError("can not watch")

    def execute(self):
        assert self._status is _Status.IDLE
        initial_read_funcs = self._read_funcs.copy()
        initial_write_funcs = self._write_funcs.copy()
        initial_end_funcs = self._end_funcs.copy()
        initial_watches = self._watches.copy()

        err_count = 0
        with self._pipe as pipe:
            while True:
                # watch
                if self._watches:
                    pipe.watch(*self._watches)

                # reading
                self._status = _Status.READING
                assert pipe.watching or len(self._read_funcs) == 0
                while self._read_funcs:
                    read_funcs = self._read_funcs
                    self._read_funcs = []
                    for func in read_funcs:
                        func()

                pipe.multi()

                # writing
                self._status = _Status.WRITING
                for func in self._write_funcs:
                    func()

                try:
                    pipe.execute()
                except WatchError:
                    if err_count > self._max_conflict:
                        raise RuntimeError('Too many watch errors!')
                    # reset
                    self._read_funcs = initial_read_funcs.copy()
                    self._write_funcs = initial_write_funcs.copy()
                    self._end_funcs = initial_end_funcs.copy()
                    self._watches = initial_watches.copy()
                    self._status = _Status.IDLE
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
