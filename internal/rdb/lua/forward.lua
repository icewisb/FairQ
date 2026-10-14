local id,token,count,score,group=ARGV[1],ARGV[2],number(ARGV[3]),number(ARGV[4]),ARGV[5]
if not id or id=='' or not token or not count or not score or not group or group=='' then return {'INTERNAL'} end
local task=taskkey(id)
if not index_types() or not typ(task,'hash') then return {'INTERNAL'} end
if redis.call('HGET',task,'state')~='retry' or redis.call('HGET',task,'attempt_token')~=token or number(redis.call('HGET',task,'retry_count'))~=count or number(redis.call('ZSCORE',retry,id))~=score then return {'STATE_CONFLICT'} end
if redis.call('HGET',task,'group')~=group or not state_indexes(id,'retry') or not group_valid(group) then return {'INTERNAL'} end
local now=nowms()
if score>now then return {'EMPTY'} end
redis.call('ZREM',retry,id)
redis.call('HSET',task,'state','pending','pending_since',now)
redis.call('LPUSH',pendingkey(group),id)
if redis.call('SADD',members,group)==1 then redis.call('RPUSH',ready,group) end
return {'OK',id}
