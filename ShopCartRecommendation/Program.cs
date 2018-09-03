﻿using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using Newtonsoft.Json;
using JsonSerializer = Utf8Json.JsonSerializer;

namespace ShopCartRecommendation
{
    class Program
    {
        static void Main(string[] args)
        {
            var file =  File.ReadAllText(@"D:\Dataset\up_ds3.json");
            var items = JsonSerializer.Deserialize<DataSet>(file);

            items.Items = items.Items.Take(5000).ToArray();

            var uniqueProductIds = items.Items.Select(x=>x.ProductId).Distinct().ToList();
            var usersCount = items.Items.Select(x => x.UserId).Distinct().ToList();

            var listProductScore = new ConcurrentBag<ProductScore>();

            var productsCounter = 0;

            foreach (var uniqueProductId in uniqueProductIds)
            {
                var boughtUsers = items.Items.Where(x => x.ProductId == uniqueProductId).Select(x=>x.UserId).Distinct().ToList();

                Console.WriteLine($"Process productsCounter - {productsCounter++} of {uniqueProductIds.Count}");

                Parallel.ForEach(uniqueProductIds, new ParallelOptions { MaxDegreeOfParallelism = 4 }, (productId) =>
                {
                    if (productId == uniqueProductId)
                        return;

                    var boughtUsers2 = items.Items.Where(x => x.ProductId == productId).Select(x => x.UserId).Distinct().ToList();

                    var commonUsers = boughtUsers.Intersect(boughtUsers2).Count();
                    var scrore = commonUsers / (double)usersCount.Count;

                    listProductScore.Add(new ProductScore
                    {
                        Product1 = uniqueProductId,
                        Product2 = productId,
                        Score = scrore
                    });
                });
            }

            using (var fs = File.OpenWrite($"ds-calc-result-{DateTime.Now:yy-MM-dd-HH-mm}.txt"))
            {
                JsonSerializer.Serialize(fs, listProductScore.OrderByDescending(x => x.Score).ToList());
            }

            //JsonSerializer.Serialize(stream, p2);

            //File.WriteAllText($"ds-calc-result-{DateTime.Now:yy-MM-dd-HH-mm}.txt", JsonConvert.SerializeObject());
            
            Console.WriteLine("Finish");
        }
    }


    public class DataSet
    {
        public UserProductModel[] Items { get; set; }
    }

    public class UserProductModel
    {
        [DataMember(Name = "user_id")]
        public string UserId { get; set; }
        [DataMember(Name = "product_id")]
        public string ProductId { get; set; }
        [DataMember(Name = "categories")]
        public string Categories { get; set; }
    }

    public class ProductScore
    {
        public string Product1 { get; set; }
        public string Product2 { get; set; }
        public double Score { get; set; }
    }

}
