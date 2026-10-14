local id,token=ARGV[1],ARGV[2]
if not id or id=='' or not token then return {'INTERNAL'} end
local now=nowms()
local code=owner(id,token,now)
if code~='OK' then return {code} end
local task=taskkey(id)
local retention=number(redis.call('HGET',task,'retention_ms'))
if not retention or now+retention>9007199254740991 then return {'INTERNAL'} end
redis.call('SREM',active,id)
redis.call('ZREM',lease,id)
if retention==0 then
    redis.call('DEL',task)
    return {'OK','deleted'}
end
redis.call('HSET',task,'state','completed','finished_at',now)
redis.call('ZADD',completed,now+retention,id)
return {'OK','completed'}
