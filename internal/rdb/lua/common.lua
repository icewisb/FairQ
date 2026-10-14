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
local function nowms()
    local t = redis.call('TIME')
    return tonumber(t[1])*1000 + math.floor(tonumber(t[2])/1000)
end
local function taskkey(id) return 'fairq:v1:task:' .. id end
local function pendingkey(group) return 'fairq:v1:pending:' .. group end
local function index_types()
    return typ(active,'set') and typ(lease,'zset') and typ(retry,'zset') and typ(archived,'zset') and typ(completed,'zset')
end
local function no_indexes(id)
    return redis.call('SISMEMBER',active,id)==0 and not redis.call('ZSCORE',lease,id)
       and not redis.call('ZSCORE',retry,id) and not redis.call('ZSCORE',archived,id) and not redis.call('ZSCORE',completed,id)
end
local function group_valid(group)
    local pending=pendingkey(group)
    if not typ(pending,'list') or not typ(ready,'list') or not typ(members,'set') then return false end
    return (redis.call('LLEN',pending)>0) == (redis.call('SISMEMBER',members,group)==1)
end
