local kind,offset,limit,retention=ARGV[1],number(ARGV[2]),number(ARGV[3]),number(ARGV[4])
local source=kind=='lease' and lease or (kind=='retry' and retry or (kind=='archived' and archived or (kind=='completed' and completed or nil)))
if not source or not offset or not limit or limit==0 or limit>100 or not retention or not typ(source,'zset') then return {'INTERNAL'} end
local total=redis.call('ZCARD',source)
if offset>=total then offset=0 end
local entries=redis.call('ZRANGE',source,offset,offset+limit-1,'WITHSCORES')
local now=nowms()
local cutoff=kind=='archived' and now-retention or now
local rows={}
for i=1,#entries,2 do
    local id,score=entries[i],entries[i+1]
    if tonumber(score)<=cutoff then
        local key=taskkey(id)
        local token,count,group,state='','','',''
        if typ(key,'hash') then
            token=redis.call('HGET',key,'attempt_token') or ''
            count=redis.call('HGET',key,'retry_count') or ''
            group=redis.call('HGET',key,'group') or ''
            state=redis.call('HGET',key,'state') or ''
        end
        table.insert(rows,{id,token,count,score,group,state})
    end
end
local scanned=#entries/2
local next=offset+scanned
if next>=total then next=0 end
return {'OK',next,scanned,rows}
