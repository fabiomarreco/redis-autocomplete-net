using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RedisAutocomplete.Net;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;

namespace RedisAutocomplete.Net.Tests
{
    [TestClass()]
    public class RedisAutoCompleteProxyTests
    {
        [TestMethod()]
        public void InsertItemsInRedisTest()
        {
            string rootPath = "PROXYTEST";
            string jsonParam = @"
{
	""index"": [
		{""wd"":""hello"",""it"":[""Item1"",""Item2""]},
		{""wd"":""world"",""it"":[""Item1""]},
		{""wd"":""holland"",""it"":[""Item2""]}
	],
	""items"":[
		{""it"":""Item1"",""sc"":1},
		{""it"":""Item2"",""sc"":2}
	]
}";

            var connection = new RedisTestConnector().Connect();
            var db = connection.GetDatabase();
            var proxy = new RedisAutoCompleteProxy(() => db);
            proxy.Clear(rootPath);
            Assert.IsFalse(db.KeyExists("PROXYTEST:ITEMS"));
            proxy.InsertItems(rootPath, jsonParam).Wait();

            var allItems = db.SortedSetScan(rootPath + ":ITEMS").ToList();
            Assert.AreEqual(2, allItems.Count);
            Assert.AreEqual(1, allItems[0].Score);
            Assert.AreEqual("Item1", (string)(allItems[0].Element));

            Assert.AreEqual(2, allItems[1].Score);
            Assert.AreEqual("Item2", (string)(allItems[1].Element));


            var hWords = db.SortedSetRangeByScore(rootPath + ":LT:h").Select(s => (string)s).ToArray();
            Assert.AreEqual(2, hWords.Length);
            Assert.AreEqual("hello", hWords[0]);
            Assert.AreEqual("holland", hWords[1]);


            var helloItems = db.SetMembers(rootPath + ":WORDS:hello").Select(s => (string)s).OrderBy(s => s).ToArray();
            Assert.AreEqual(2, helloItems.Length);
            Assert.AreEqual("Item1", helloItems[0]);
            Assert.AreEqual("Item2", helloItems[1]);

            proxy.Clear(rootPath);
            Assert.IsFalse(db.KeyExists(rootPath + ":ITEMS"));
            connection.Close();
        }

        [TestMethod()]
        public void SearchTest()
        {
            string rootPath = "PROXYTEST";
            string jsonParam = @"
{
	""index"": [
		{""wd"":""hello"",""it"":[""Item1"",""Item2""]},
		{""wd"":""world"",""it"":[""Item1""]},
		{""wd"":""holland"",""it"":[""Item2""]}
	],
	""items"":[
		{""it"":""Item1"",""sc"":1},
		{""it"":""Item2"",""sc"":2}
	]
}";
            var connection = new RedisTestConnector().Connect();
            var proxy = new RedisAutoCompleteProxy(() => connection.GetDatabase());
            proxy.Clear(rootPath);
            proxy.InsertItems(rootPath, jsonParam).Wait();

            var result = proxy.Search(rootPath,JsonConvert.SerializeObject(new {prefixes = new[] {"hol"}}), 15, 10).Result;
            Assert.AreEqual(1, result.Length);
            Assert.AreEqual("Item2", result[0]);
            connection.Close();
        }



        [TestMethod()]
        public void SearchAzulTest()
        {
            string rootPath = "PROXYTEST";
            string jsonParam = @"
{
	""index"": [
		{""wd"":""arte"",""it"":[""Item1"",""Item2""]},
		{""wd"":""aula"",""it"":[""Item1""]},
		{""wd"":""azul"",""it"":[""Item3""]},
		{""wd"":""bola"",""it"":[""Item2""]},
		{""wd"":""casa"",""it"":[""Item2""]}
	],
	""items"":[
		{""it"":""Item1"",""sc"":1},
		{""it"":""Item2"",""sc"":2},
		{""it"":""Item3"",""sc"":3}
	]
}";
            var connection = new RedisTestConnector().Connect();
            var proxy = new RedisAutoCompleteProxy(() => connection.GetDatabase());
            proxy.Clear(rootPath);
            proxy.InsertItems(rootPath, jsonParam).Wait();

            var result = proxy.Search(rootPath, JsonConvert.SerializeObject(new { prefixes = new[] { "a" } }), 15, 10).Result;
            Assert.AreEqual(3, result.Length);
            Assert.IsTrue(result.Contains("Item1"));
            Assert.IsTrue(result.Contains("Item2"));
            Assert.IsTrue(result.Contains("Item3"));

            result = proxy.Search(rootPath, JsonConvert.SerializeObject(new { prefixes = new[] { "az" } }), 15, 10).Result;
            Assert.AreEqual(1, result.Length);
            Assert.IsTrue(result.Contains("Item3"));

            connection.Close();
        }
    }
}
