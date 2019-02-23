counter = 0
lines = {}
io.input("./wlist.txt")
for line in io.lines() do
    table.insert(lines, line)
end
numlines = table.getn(lines)

function string.random(c)
    return lines[(c%numlines)+1]
end

request = function()
    counter = math.random(1, numlines)
    rstr = string.random(counter)
    rlen = string.len(rstr)
    if rlen > 8 then
        rlen = 8
    end
    num = math.random(1, rlen)
    rstr = string.sub(rstr, 0, num)
    path = "/1/indexes/reddit/query?x-marlin-application-id=DAlFjWFg&x-marlin-rest-api-key=b8xKr1jExeB2UxmYhUJKsNpV4zNLx50V"
    wrk.method = "POST"
    wrk.body   = "{\"q\":\"".. rstr .. "\"}"
    wrk.headers["Content-Type"] = "application/json"
    return wrk.format("POST", path)
end
