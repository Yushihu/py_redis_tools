from itertools import chain

from redis.commands.core import ScriptCommands

from .scripts import HSETXX_SHA, HPATCH_SHA, LUA_HSETXX, LUA_HPATCH


class Extension(ScriptCommands):
    def hsetxx(self, key, field, value):
        return hsetxx(self, key, field, value)

    def hpatch(self, key, mapping: dict):
        return hpatch(self, key, mapping)

    def install_extension(self):
        self.script_load(LUA_HSETXX)
        self.script_load(LUA_HPATCH)


def hsetxx(client, key, field, value):
    return client.evalsha(HSETXX_SHA, 1, key, field, value)


def hpatch(client, key, mapping: dict):
    return client.evalsha(HPATCH_SHA, 1, key, *chain(*mapping.items()))
