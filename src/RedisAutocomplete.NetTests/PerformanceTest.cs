using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Mime;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using RedisAutocomplete.Net;
using RedisAutocomplete.Net.Tests;

namespace RedisAutocomplete.NetTests
{
    [TestClass]
    public class PerformanceTest
    {

        [TestMethod]
        public void InsertPerformance900KTest()
        {
            var connection = new RedisTestConnector().Connect();
            try
            {
                var proxy = new RedisAutoCompleteProxy(connection.GetDatabase());
                string root = "PERFORMANCETEST";
                var index = new RedisAutoCompleteIndex(proxy, root);
                index.Clear();
                var db = connection.GetDatabase();
                Assert.IsFalse(db.KeyExists(root + ":ITEMS"));
                var items = LoadAllItems();
                var sw = new Stopwatch();

                sw.Start();
                var tsk = index.Add(items);
                tsk.Wait();
                sw.Stop();

                Assert.IsTrue(sw.Elapsed.TotalSeconds < 120);
                Trace.WriteLine("INSERT ITEMS TIME: " + sw.Elapsed.ToString());
                Assert.AreEqual(items.Length, db.SortedSetLength(root + ":ITEMS"));
                sw.Restart();
                var result = index.Search("hol").Result;
                sw.Stop();

                Assert.IsTrue(result.Any());
                Trace.WriteLine("FIRST QUERY TIME: " + sw.Elapsed.ToString());
                Assert.IsTrue(sw.ElapsedMilliseconds < 500);

                sw.Restart();
                var result2 = index.Search("hol").Result;
                sw.Stop();
                
                Assert.IsTrue(result2.Any());
                
                Assert.AreEqual(result.Length, result2.Length);
                Trace.WriteLine("SECOND QUERY TIME: " + sw.Elapsed.ToString());
                Assert.IsTrue(sw.ElapsedMilliseconds < 50);

                index.Clear();
            }
            finally
            { connection.Close();  }
            
            //index.Clear();
        }

        private static AutoCompleteItem[] LoadAllItems()
        {
            var itens =
                File.ReadAllLines(@"c:\temp\itens.csv", Encoding.Default)
                    .Select(l => l.Split('\t'))
                    .Select(x =>
                        new AutoCompleteItem()
                        {
                            Item = JsonConvert.SerializeObject(new { id = x[0], c = x[1] }), Priority = int.Parse(x[1]), Text = x[2]
                        })
                    .ToArray();
            return itens;
        }
    }
}
