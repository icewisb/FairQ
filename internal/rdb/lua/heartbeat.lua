local duration=number(ARGV[1])
if not duration or duration==0 or (#ARGV-1)%2~=0 or #ARGV>201 then return {'INTERNAL'} end
if not index_types() then return {'INTERNAL'} end
local now=nowms()
local deadline=now+duration
if deadline>9007199254740991 then return {'INTERNAL'} end
local result={'OK'}
for i=2,#ARGV,2 do
    local id,token=ARGV[i],ARGV[i+1]
    local code=owner(id,token,now)
    if code=='OK' then redis.call('ZADD',lease,deadline,id) end
    table.insert(result,{id,code,code=='OK' and tostring(deadline) or ''})
end
return result
