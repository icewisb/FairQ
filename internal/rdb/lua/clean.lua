local id,state,token,score,retention=ARGV[1],ARGV[2],ARGV[3],number(ARGV[4]),number(ARGV[5])
local source=state=='archived' and archived or (state=='completed' and completed or nil)
if not source or not id or id=='' or not token or not score or not retention or not index_types() then return {'INTERNAL'} end
local current=number(redis.call('ZSCORE',source,id))
if current~=score then return {'STATE_CONFLICT'} end
local task=taskkey(id)
if not typ(task,'hash') or redis.call('EXISTS',task)==0 then return {'INTERNAL'} end
if redis.call('HGET',task,'state')~=state or redis.call('HGET',task,'attempt_token')~=token then return {'STATE_CONFLICT'} end
if not state_indexes(id,state) then return {'INTERNAL'} end
local now=nowms()
local cutoff=state=='archived' and now-retention or now
if score>cutoff then return {'EMPTY'} end
redis.call('ZREM',source,id)
redis.call('DEL',task)
return {'OK',id}
