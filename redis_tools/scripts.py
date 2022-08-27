from .utils import sha1

LUA_HSETXX = '''
local key = KEYS[1]
local field = ARGV[1]
local value = ARGV[2]
local exists = redis.call('HEXISTS', key, field)
if exists == 0 then
    return 0
end
redis.call('HSET', key, field, value)
return 1
'''
HSETXX_SHA = sha1(LUA_HSETXX)

LUA_HPATCH = '''
local key = KEYS[1]
local fv = ARGV
local exists = redis.call('EXISTS', key)
if exists == 0 then
    return 0
end
redis.call('HSET', key, unpack(fv))
return 1
'''
HPATCH_SHA = sha1(LUA_HPATCH)
