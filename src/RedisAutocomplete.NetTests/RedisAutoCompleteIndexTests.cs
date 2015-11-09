using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RedisAutocomplete.Net;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using StackExchange.Redis;

namespace RedisAutocomplete.Net.Tests
{
    [TestClass()]
    public class RedisAutoCompleteIndexTests
    {
        [TestMethod()]
        public void AddItemsToIndexCreatingJSONTest()
        {
            string rootPath = "RedisAutoCompleteIndexBuilderTests";

            var mqProxy = new Mock<IRedisAutoCompleteProxy>();

            var objParam = new
            {
                index = new[] {new {wd = string.Empty, it = new string[] {}}},
                items = new[] {new {it = string.Empty, sc = default(int)}}
            };

            mqProxy.Setup(s => s.InsertItems(It.IsAny<string>(), It.IsAny<string>()))
                .Callback((string path, string json) =>
                {
                    objParam = JsonConvert.DeserializeAnonymousType(json, objParam);
                }).Returns(Task.FromResult(0));


            var index = new RedisAutoCompleteIndex(mqProxy.Object, rootPath);
            //var index = new RedisAutoCompleteIndex(redis.GetDatabase(), rootPath);
            index.Clear();
            var items = new[]
            {
                new AutoCompleteItem() {Priority = 1, Text = "Hello World!", Item = "Item1"},
                new AutoCompleteItem() {Priority = 2, Text = "Hello (Holland)", Item = "Item2"},
            };

            index.Add(items).Wait();
            mqProxy.VerifyAll();

            var jidx = objParam.index.ToDictionary(x => x.wd, x => x.it);
            AssertDictEqual(jidx, new Dictionary<string, string[]>()
            {
                {"hello", new string[] {"Item1", "Item2"}},
                {"holland", new string[] {"Item2"}},
                {"world", new string[] {"Item1"}},
            });

            var jitems = objParam.items.ToDictionary(x => x.it, x => x.sc);
            Assert.AreEqual(2, jitems.Count);
            Assert.AreEqual(1, jitems["Item1"]);
            Assert.AreEqual(2, jitems["Item2"]);
        }

        private void AssertDictEqual(Dictionary<string, string[]> actual, Dictionary<string, string[]> expected)
        {
            Assert.AreEqual(actual.Count, expected.Count);
            foreach (var item in expected)
            {
                Assert.IsTrue(actual.ContainsKey(item.Key));
                var actualValues = actual[item.Key];
                var expectedValues = item.Value;
                Assert.AreEqual(expectedValues.Length, actualValues.Length);
                foreach (var expectedValue in expectedValues)
                    Assert.IsTrue(actualValues.Contains(expectedValue));
            }
        }

        [TestMethod()]
        public void RemoveItemTest()
        {
            var connector = new RedisTestConnector();
            var redis = connector.Connect();
            string rootPath = "RedisAutoCompleteIndexBuilderTests";

            var proxy = new RedisAutoCompleteProxy(redis.GetDatabase());
            //var index = new RedisAutoCompleteIndex(redis.GetDatabase(), rootPath);
            var index = new RedisAutoCompleteIndex(proxy, rootPath);
            index.Clear();
            var items = new[]
            {
                new AutoCompleteItem() {Priority = 1, Text = "Hello World!", Item = "Item1"},
                new AutoCompleteItem() {Priority = 2, Text = "Hello (Holland)", Item = "Item2"},
                new AutoCompleteItem() {Priority = 1, Text = "Hello Brazil!", Item = "Item3"},
            };

            index.Add(items).Wait();
            var db = redis.GetDatabase();
            var allItems = db.SortedSetRangeByScore(rootPath + ":ITEMS").Select(s => (string) s).ToArray();
            Assert.AreEqual(3, allItems.Length);
            Assert.IsTrue(allItems.Contains("Item2"));

            index.RemoveItem("Item2").Wait();
            
            allItems = db.SortedSetRangeByScore(rootPath + ":ITEMS").Select(s => (string)s).ToArray();
            Assert.AreEqual(2, allItems.Length);
            Assert.IsFalse(allItems.Contains("Item2"));
            index.Clear();
            redis.Close();
        }

        [TestMethod()]
        public void IncreasePriorityTest()
        {
            var connector = new RedisTestConnector();
            var redis = connector.Connect();
            string rootPath = "RedisAutoCompleteIndexBuilderTests";

            var proxy = new RedisAutoCompleteProxy(redis.GetDatabase());
            //var index = new RedisAutoCompleteIndex(redis.GetDatabase(), rootPath);
            var index = new RedisAutoCompleteIndex(proxy, rootPath);
            
            index.Clear();
            var items = new[]
            {
                new AutoCompleteItem() {Priority = 1, Text = "Hello World!", Item = "Item1"},
                new AutoCompleteItem() {Priority = 2, Text = "Hello (Holland)", Item = "Item2"},
                new AutoCompleteItem() {Priority = 1, Text = "Hello Brazil!", Item = "Item3"},
            };

            index.Add(items);

            index.IncreasePriority("Item3");
            var db = redis.GetDatabase();
            var item = (string)db.SortedSetRangeByScore(rootPath + ":ITEMS").First();
            Assert.AreEqual("Item3", item);
            index.Clear();
            redis.Close();
        }

    

    }



    public class RedisTestConnector
    {
        public IConnectionMultiplexer Connect()
        {
            try
            {
                var result = ConnectionMultiplexer.Connect("localhost,syncTimeout=10000");
                if (!result.IsConnected)
                    Assert.Inconclusive("Redis not connected");
                return result;
            }
            catch (RedisConnectionException)
            {
                Assert.Inconclusive("Redis not connected");
                throw;
            }
        }

    }
}
