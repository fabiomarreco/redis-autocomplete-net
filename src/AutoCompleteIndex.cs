using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using StackExchange.Redis;

namespace RedisAutocomplete.Net
{
    // funcionalidades: 
    //   1- Build de indices
    //   2 - Recuperacao de objetos

    public class RedisAutoCompleteIndex : IAutoCompleteIndex
    {
        private const int MAX_BATCH = 5000;
        private readonly IRedisAutoCompleteProxy _proxy;
        private readonly string _rootPath;
        private readonly int _maxResultCount;
        private readonly long _expire;
        private char[] _separadores;

        public RedisAutoCompleteIndex(IRedisAutoCompleteProxy proxy, string rootPath, int maxResultCount = 15, long expire = 60)
        {
            if (proxy == null)
                throw new ArgumentException("Redis proxy cannot be null", "proxy");

            if (string.IsNullOrEmpty(rootPath))
                throw new ArgumentException("Root path cannot be empty", "rootPath");

            _proxy = proxy;
            _rootPath = rootPath;
            _maxResultCount = maxResultCount;
            _expire = expire;
            _separadores = Constants.WordSeparators.ToCharArray();
        }

        public Task Clear()
        {
            return _proxy.Clear(_rootPath);
        }

        public Task Add(params AutoCompleteItem[] items)
        {

            var batched = items.Select((item, idx) => new {idx, item}).GroupBy(v => (int) (v.idx/MAX_BATCH), c => c.item);
            List<Task> tasks = new List<Task>();
            foreach (var batch in batched)
            {
                var parameter = CreateInsert(batch.ToArray());
                var tsk = _proxy.InsertItems(_rootPath, parameter);
                tasks.Add(tsk);
            }

            return Task.WhenAll(tasks);
            /*
            var batch = _db.CreateBatch();
            foreach (var witems in byWord)
            {
                batch.SortedSetAddAsync(string.Format("{0}:LT:{1}", _rootPath, witems.Key.Substring(0, 1)), witems.Key, 0);
                foreach (var item in witems)
                    batch.SetAddAsync(string.Format("{0}:WORDS:{1}", _rootPath, witems.Key), item.ItemKey);
            }

            batch.Execute();
            batch = _db.CreateBatch();
            foreach (var item in items)
               batch.SortedSetAddAsync(string.Format("{0}:ITEMS", _rootPath), item.ItemKey, item.Priority);

            batch.Execute();
            //return result;
            */
            //return Task.FromResult(0);
        }

        private string CreateInsert(AutoCompleteItem[] items)
        {
            
            var byWord = (from item in items
                from word in item.Text.ToLower().Split(_separadores).Select(s => s.Trim()).Where(s => s.Length > 0).Distinct()
                group item by word).ToList();

            var jsonObj = new
            {
                vocabulary = from w in byWord
                    group w.Key by w.Key.Substring(0, 1)
                    into grpKey
                    select new
                    {
                        letter = grpKey.Key,
                        words = grpKey.ToArray()
                    },
                index = from w in byWord
                    select new {wd = w.Key, it = w.Select(s => s.ItemKey).ToArray()},
                items = from item in items
                    select new
                    {
                        it = item.ItemKey,
                        sc = item.Priority
                    }
            };

            string parameter = JsonConvert.SerializeObject(jsonObj);
            return parameter;
        }

        public Task<long> RemoveItem(params string[] items)
        {
            var values = items.Select(s => (RedisValue) s).ToArray();
            return _proxy.RemoveItems(_rootPath, values);
        }

        public Task IncreasePriority(string item)
        {
            return _proxy.IncreasePriority(_rootPath, item);
        }


        public Task<string[]> Search(string searchTerm)
        {
            var searchWords =
                searchTerm.ToLower().Split(_separadores).Select(s => s.Trim()).Where(s => s.Length > 0).Distinct().ToArray();

            var jsonList = JsonConvert.SerializeObject(new {prefixes = searchWords.ToArray()});
            return _proxy.Search(_rootPath, jsonList, _maxResultCount, _expire);
        }
    }
}
