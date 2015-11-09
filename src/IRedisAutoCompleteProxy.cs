using System.IO;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace RedisAutocomplete.Net
{
    public interface IRedisAutoCompleteProxy
    {
        Task InsertItems(string rootPath, string jsonParameters);
        Task<long> RemoveItems(string rootPath, RedisValue[] values);
        Task IncreasePriority(string rootPath, string item);
        Task Clear(string rootPath);
        Task<string[]> Search(string rootPath, string jsonTermList, int maxResultCount, long cacheExpire);
    }

    public class RedisAutoCompleteProxy : IRedisAutoCompleteProxy
    {
        private readonly IDatabase _db;

        private string GetScript(string name)
        {
            var stream = this.GetType().Assembly.GetManifestResourceStream(string.Format("RedisAutocomplete.Net.redis_lua.{0}.lua", name));
            if (stream == null)
                return string.Empty;

            using (var sr = new StreamReader(stream))
            {
                return sr.ReadToEnd();
            }
        }

        public RedisAutoCompleteProxy(IDatabase db)
        {
            _db = db;
        }

        public Task InsertItems(string rootPath, string jsonParameters)
        {
            string script = GetScript("addindex");
            return _db.ScriptEvaluateAsync(script, new RedisKey[] {}, new RedisValue[] {rootPath, jsonParameters});
        }

        public Task<long> RemoveItems(string rootPath, RedisValue[] values)
        {
            return _db.SortedSetRemoveAsync(rootPath + ":ITEMS", values);

        }

        public Task IncreasePriority(string rootPath, string item)
        {
            return _db.SortedSetIncrementAsync(rootPath + ":ITEMS", item, -1);
        }

        public Task Clear(string rootPath)
        {
            return _db.ScriptEvaluateAsync(
@"local keys = redis.call('keys', ARGV[1]) 
for i=1,#keys,5000 do
	redis.call('del', unpack(keys, i, math.min(i+4999, #keys))) 
end 
return keys", new RedisKey[] { }, new RedisValue[] { rootPath + ":*" });

        }

        public async Task<string[]> Search(string rootPath, string jsonTermList, int maxResultCount, long cacheExpire)
        {
            var script = GetScript("searchitems");
            RedisResult result =
                await
                    _db.ScriptEvaluateAsync(script, new RedisKey[] {},
                        new RedisValue[] {rootPath, jsonTermList, maxResultCount, cacheExpire});

            string[] items = (string[]) result;
            return items;
        }
    }
}