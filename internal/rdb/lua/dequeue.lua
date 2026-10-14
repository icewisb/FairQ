local token,duration=ARGV[1],number(ARGV[2])
if not token or token=='' or not duration or duration==0 then return {'INTERNAL'} end
if not typ(ready,'list') or not typ(members,'set') or not index_types() then return {'INTERNAL'} end
local group=redis.call('LINDEX',ready,0)
if not group then return {'EMPTY'} end
local pending=pendingkey(group)
if not typ(pending,'list') then return {'INTERNAL'} end
local id=redis.call('LINDEX',pending,-1)
if not id then
    if not evidence('dequeue','EMPTY_GROUP',ready,'',group,'','') then return {'INTERNAL'} end
    redis.call('LPOP',ready)
    redis.call('SREM',members,group)
    return {'CORRUPT_REFERENCE'}
end
if redis.call('SISMEMBER',members,group)~=1 then return {'INTERNAL'} end
local task=taskkey(id)
if not typ(task,'hash') then return {'INTERNAL'} end
local state=redis.call('HGET',task,'state')
local bad=false
if redis.call('EXISTS',task)==0 then
    bad=true
elseif state~='pending' and state_indexes(id,state) then
    if state~='active' or (redis.call('HGET',task,'attempt_token') or '')~='' then bad=true end
end
if bad then
    if not evidence('dequeue','INVALID_PENDING',pending,id,group,state,'') then return {'INTERNAL'} end
    redis.call('RPOP',pending)
    if redis.call('LLEN',pending)==0 then
        redis.call('LPOP',ready)
        redis.call('SREM',members,group)
    end
    return {'CORRUPT_REFERENCE'}
end
if state~='pending' or redis.call('HGET',task,'group')~=group or not state_indexes(id,'pending') then return {'INTERNAL'} end
local fields=redis.call('HMGET',task,'retry_count','max_retry','timeout_ms','retention_ms')
local count,maxretry,timeout,retention=number(fields[1]),number(fields[2]),task_duration(fields[3]),task_duration(fields[4])
if not count or not maxretry or not timeout or not retention then return {'INTERNAL'} end
local msg=redis.call('HGET',task,'msg') or ''
local now=nowms()
local deadline=now+duration
if deadline>9007199254740991 then return {'INTERNAL'} end
redis.call('LPOP',ready)
redis.call('RPOP',pending)
redis.call('HSET',task,'state','active','attempt_token',token,'started_at',now)
redis.call('HDEL',task,'pending_since')
redis.call('SADD',active,id)
redis.call('ZADD',lease,deadline,id)
if redis.call('LLEN',pending)>0 then redis.call('RPUSH',ready,group) else redis.call('SREM',members,group) end
return {'OK',id,group,msg,token,deadline,timeout,count,maxretry}
