local id,group,msg = ARGV[1],ARGV[2],ARGV[3]
local maxretry,timeout,retention = number(ARGV[4]),task_duration(ARGV[5]),task_duration(ARGV[6])
if not id or id=='' or not group or group=='' or not msg or not maxretry or not timeout or not retention then return {'INTERNAL'} end
local task,pending=taskkey(id),pendingkey(group)
if redis.call('EXISTS',task)==1 then return {'TASK_ID_CONFLICT'} end
if not index_types() or not state_indexes(id,'pending') or not group_valid(group) then return {'INTERNAL'} end
local now=nowms()
redis.call('HSET',task,'msg',msg,'group',group,'state','pending','retry_count',0,
    'max_retry',maxretry,'timeout_ms',timeout,'retention_ms',retention,'enqueued_at',now,'pending_since',now)
redis.call('LPUSH',pending,id)
if redis.call('SADD',members,group)==1 then redis.call('RPUSH',ready,group) end
return {'OK',id,now}
