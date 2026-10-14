local id,token,count,score,delay=ARGV[1],ARGV[2],number(ARGV[3]),number(ARGV[4]),number(ARGV[5])
if not id or id=='' or not token or not score or not delay or not index_types() then return {'INTERNAL'} end
local task=taskkey(id)
if not typ(task,'hash') then return {'INTERNAL'} end
local current=number(redis.call('ZSCORE',lease,id))
if not current or current~=score then return {'STATE_CONFLICT'} end
local now=nowms()
if current>now then return {'EMPTY'} end
local state=redis.call('HGET',task,'state')
if redis.call('SISMEMBER',active,id)==0 then
    local orphan=redis.call('EXISTS',task)==0
    if not orphan and (state=='retry' or state=='archived' or state=='completed') then
        local target=state=='retry' and retry or (state=='archived' and archived or completed)
        local others=(state=='retry' or not redis.call('ZSCORE',retry,id)) and (state=='archived' or not redis.call('ZSCORE',archived,id)) and (state=='completed' or not redis.call('ZSCORE',completed,id))
        orphan=redis.call('ZSCORE',target,id)~=false and others
    end
    if orphan then
        if not evidence('recover','ORPHAN_LEASE',lease,id,'',state,tostring(score)) then return {'INTERNAL'} end
        redis.call('ZREM',lease,id)
        return {'CORRUPT_REFERENCE'}
    end
    return {'INTERNAL'}
end
if state~='active' then return {'INTERNAL'} end
if token=='' or redis.call('HGET',task,'attempt_token')~=token or not count or number(redis.call('HGET',task,'retry_count'))~=count then return {'STATE_CONFLICT'} end
if not state_indexes(id,'active') then return {'INTERNAL'} end
return fail_transition(id,now,delay,'LEASE_EXPIRED',false)
