local root = ARGV[1] --- 'SI'
local input = ARGV[2] --- 'SI'
--- local root = 'SI'
---local input = ' {"vocabulary":[ {"letter": "a","words": ["a", "abacate", "abacaxi"]},{"letter": "h","words": ["hello", "holland", "homer"]}],"index":[{"wd": "a","it": ["1", "10", "20"]},{"wd": "abacate","it": ["10", "33"]},{"wd": "homer","it": ["10", "44"]}],"items":[{ "it": "10", "sc": "1" }]}'

local config = cjson.decode(input)

for k,v in pairs(config["items"]) do
	redis.call('ZADD', root .. ':ITEMS', v["sc"], v["it"])
end

for k,v in pairs(config["index"]) do
	local items = v["it"]
	local step = 1000
	local wordKey = root .. ':WORDS:' .. v["wd"]

	for i=1,#items, 1000 do
		redis.call('SADD', wordKey, unpack(items, i, math.min(i + step -1, #items)))
	end

	local word = v["wd"]
	local firstLetter = word:sub(1,1)
	redis.call('ZADD', root .. ':LT:' .. firstLetter, 0, word)
end

