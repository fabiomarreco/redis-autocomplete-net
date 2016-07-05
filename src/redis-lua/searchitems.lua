----------------------------------------------------------------------------------
--- ItemsSearch.lua - Fabio Catunda Marreco - 2015-11-06
---
--- DESCRIPTION:
--- 	Redis script in lua for querying words through prefixes with high 
--- performance.
---   	This script requires an existing REDIS database with the following format:
--- 
---  [ROOT] : LT : [letters a-z]
---       > Sorted sets with all available words in a given letter prefix
---
---  [ROOT] : WORDS : [ Available words ]
---       > A set for each available word containing a list of objects to be searched.
---
---   Given a list of prefixes for words, this script searches for all available words
---  with the given prefix by doing a lex search in redis 'ZRANGEBYLEX' and creates a new 
---  key with the UNION of all available word for each prefix.
--- ... continue...
-----------------------------------------------------------------------------------


local root = ARGV[1] --- 'SI'
local arg = ARGV[2] --- '{ "prefixes": [ "marf", "a" ] }'
--local arg =  '{ "prefixes": [ "marf", "a" ] }'

local maxResult = ARGV[3] --- 15
local cacheExpire = ARGV[4] --- 30 --sec

-- Parse of all prefixes given as parameters
local result = {}
local searchPrefixes = cjson.decode(arg)["prefixes"]


--- Result key
local resultSetKey = root .. ':CACHE:RESULTS:'
for k,v in pairs(searchPrefixes) do resultSetKey = resultSetKey .. v .. '+' end


-- #### BUSCA LISTA DE PALAVRAS VÁLIDAS INICIALIZANDO COM UM INICIO
local function get_words(sword)
	local c = sword:sub(1,1) -- Primeiro caractere
	local key = root .. ':LT:' .. c
	local sstart = '[' .. sword
	local send = '(' .. sword:sub(1,-2) .. string.char(string.byte(sword:sub(-1))+1)
	--local words = redis.call('ZRANGEBYLEX', key, sstart, send, 'LIMIT', 0, maxResult)
	local words = redis.call('ZRANGEBYLEX', key, sstart, send)
	return words
end


if redis.call('EXISTS', resultSetKey) == 0 then
	local prefixKeys = {}
	for i, sword in pairs(searchPrefixes) do
		local words = get_words(sword)
		local prefixItemsKey = root .. ':CACHE:PRF:' .. sword
		prefixKeys[#prefixKeys+1] = prefixItemsKey 
		if redis.call('EXISTS', prefixItemsKey) == 0 then
			local allKeys = {}
			for j, word in pairs(words) do
				local wordKey = root .. ':WORDS:' .. word
				allKeys[#allKeys+1] = wordKey
				if #allKeys > 1000 then
					redis.call('SUNIONSTORE', prefixItemsKey, prefixItemsKey, unpack(allKeys))
					allKeys = {}
				end
			end
			redis.call('SUNIONSTORE', prefixItemsKey, prefixItemsKey, unpack(allKeys))
			redis.call('EXPIRE', prefixItemsKey, cacheExpire)
		end
	end
	redis.call("SINTERSTORE", resultSetKey, unpack (prefixKeys))
	redis.call('ZINTERSTORE', resultSetKey, 2, resultSetKey, root .. ':ITEMS', 'WEIGHTS', 0, 1)
	redis.call('EXPIRE', resultSetKey, cacheExpire)
end

local result = redis.call ('ZRANGEBYSCORE', resultSetKey, '-inf', '+inf', 'LIMIT', '0', maxResult)

return result