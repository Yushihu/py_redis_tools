from redis.asyncio.client import Redis as _Redis, Pipeline as _Pipeline

from .extension import Extension


class Pipeline(Extension, _Pipeline):
    pass


class Redis(Extension, _Redis):
    def pipeline(
            self,
            transaction: bool = True,
            shard_hint: str | None = None
    ):
        return Pipeline(
            self.connection_pool, self.response_callbacks, transaction, shard_hint
        )
