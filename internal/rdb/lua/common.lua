local ready, members, active, lease, retry, archived, completed, badrefs = unpack(KEYS)
local function typ(key, expected)
    local t = redis.call('TYPE', key).ok
    return t == 'none' or t == expected
end
local function number(s)
    local n = tonumber(s)
    if not n or n < 0 or n > 9007199254740991 or n ~= math.floor(n) then return nil end
    return n
end
local function task_duration(s)
    local n = number(s)
    -- Go time.Duration stores nanoseconds; persisted task durations are milliseconds.
    if not n or n > 9223372036854 then return nil end
    return n
end
local function nowms()
    local t = redis.call('TIME')
    return tonumber(t[1])*1000 + math.floor(tonumber(t[2])/1000)
end
local function taskkey(id) return 'fairq:v1:task:' .. id end
local function pendingkey(group) return 'fairq:v1:pending:' .. group end
local function index_types()
    return typ(active,'set') and typ(lease,'zset') and typ(retry,'zset') and typ(archived,'zset') and typ(completed,'zset')
end
local function group_valid(group)
    local pending=pendingkey(group)
    if not typ(pending,'list') or not typ(ready,'list') or not typ(members,'set') then return false end
    return (redis.call('LLEN',pending)>0) == (redis.call('SISMEMBER',members,group)==1)
end
local function state_indexes(id,state)
    local a=redis.call('SISMEMBER',active,id)==1
    local l=redis.call('ZSCORE',lease,id)~=false
    local r=redis.call('ZSCORE',retry,id)~=false
    local ar=redis.call('ZSCORE',archived,id)~=false
    local c=redis.call('ZSCORE',completed,id)~=false
    if state=='pending' then return not a and not l and not r and not ar and not c end
    if state=='active' then return a and l and not r and not ar and not c end
    if state=='retry' then return r and not a and not l and not ar and not c end
    if state=='archived' then return ar and not a and not l and not r and not c end
    if state=='completed' then return c and not a and not l and not r and not ar end
    return false
end
local function evidence(op,reason,source,id,group,state,score)
    if not typ(badrefs,'list') then return false end
    local record=cjson.encode({at=nowms(),operation=op,reason=reason,source_key=source,task_id=id or '',group=group or '',state=state or '',score=score or ''})
    local res=redis.pcall('RPUSH',badrefs,record)
    return type(res)~='table' or not res.err
end
local function owner(id,token,now)
    local key=taskkey(id)
    if not index_types() or not typ(key,'hash') then return 'INTERNAL' end
    if redis.call('HGET',key,'state')~='active' or token=='' or redis.call('HGET',key,'attempt_token')~=token then return 'STATE_CONFLICT' end
    if not state_indexes(id,'active') then return 'STATE_CONFLICT' end
    local deadline=number(redis.call('ZSCORE',lease,id))
    if not deadline then return 'INTERNAL' end
    if deadline<=now then return 'LEASE_EXPIRED' end
    return 'OK'
end
local function fail_transition(id,now,delay,reason,decode)
    local task=taskkey(id)
    local count,maxretry=number(redis.call('HGET',task,'retry_count')),number(redis.call('HGET',task,'max_retry'))
    if not count or not maxretry or count>=9007199254740991 then return {'INTERNAL'} end
    local k=count+1
    local capped=15000
    local i=1
    while i<k and capped<3600000 do capped=math.min(capped*2,3600000);i=i+1 end
    if not decode and (not delay or delay<capped or delay>capped*1.1) then return {'INTERNAL'} end
    local state,score='retry',now+(delay or 0)
    if decode or k>maxretry then state='archived';score=now end
    if score>9007199254740991 then return {'INTERNAL'} end
    if decode then k=count;reason='DECODE_FAILED' end
    redis.call('SREM',active,id)
    redis.call('ZREM',lease,id)
    redis.call('HSET',task,'state',state,'retry_count',k,'failed_at',now,'error',reason)
    if state=='archived' then
        redis.call('HSET',task,'finished_at',now)
        redis.call('ZADD',archived,score,id)
    else
        redis.call('ZADD',retry,score,id)
    end
    return {'OK',state,k,score}
end
