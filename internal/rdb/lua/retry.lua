local id,token,count,delay,reason,mode=ARGV[1],ARGV[2],number(ARGV[3]),number(ARGV[4]),ARGV[5],ARGV[6]
if not id or id=='' or not token or not count or not delay or not reason or (mode~='error' and mode~='decode') or not index_types() then return {'INTERNAL'} end
local now=nowms()
local code=owner(id,token,now)
if code~='OK' then return {code} end
if number(redis.call('HGET',taskkey(id),'retry_count'))~=count then return {'STATE_CONFLICT'} end
return fail_transition(id,now,delay,reason,mode=='decode')
