//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------
namespace Microsoft.Azure.Cosmos.Samples.Mongo
{
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using MongoDB.Bson;
    using MongoDB.Driver;
    using MongoDB.Driver.Core.Servers;

    /// <summary>
    /// This sample demonstrates some multi-master scenarios over the Cosmos DB MongoDB API.
    /// Utility code that may be useful in other applications is kept in MongoMultiMasterHelpers.cs.
    /// Code in Program.cs is specific to the demonstration scenarios.
    /// </summary>
    internal static class Program
    {
        private const string HelpText = @"
Performs a demonstration of multi-master scenarios against the account identified 
by the supplied connection string, database, and collection.

To use, supply a value for 'connectionstring' in the application config file.

Use a mongodb:// style connection string, such as the one from the Azure Portal. 
To specify a preferred write region, append the appName=[app]@<azure region> option.

  Example with no preferred write region:
    mongodb://user:password@example.documents.azure.com:10255/?ssl=true&replicaSet=globaldb

  Examples with preferred write region of 'East US':
    mongodb://user:password@example.documents.azure.com:10255/?ssl=true&replicaSet=globaldb&appName=@East US
    mongodb://user:password@example.documents.azure.com:10255/?ssl=true&replicaSet=globaldb&appName=myapp@East US

  The available region closest geographically to the preferred write region will be presented to the MongoClient 
  as the PRIMARY replica set member and will accept writes.

  Note that database and collection creation must be performed using the default write region.
";

        /// <summary>
        /// Main entry point
        /// </summary>
        public static void Main(string[] args)
        {
            Program.MainAsync(args).Wait();
        }

        /// <summary>
        /// Main entry point (async)
        /// </summary>
        private static async Task MainAsync(string[] args)
        {
            string mongoConnectionStringRaw = ConfigurationManager.AppSettings["connectionstring"];

            if (string.IsNullOrWhiteSpace(mongoConnectionStringRaw))
            {
                Console.WriteLine(Program.HelpText);
                return;
            }

            try
            {
                MongoUrl mongoUrl = new MongoUrl(mongoConnectionStringRaw);
                MongoClientSettings mongoClientSettings = MongoClientSettings.FromUrl(mongoUrl);

                // Write documents to region specified in connection string
                MongoClient mongoClient = new MongoClient(mongoClientSettings);
                await Program.WaitForConnectionsToAllServersAsync(mongoClient, TimeSpan.FromSeconds(30));
                await Program.ExecuteWriteWorkloadAsync(mongoClient, Guid.NewGuid());

                // Discover all available regions
                Dictionary<string, MongoClient> regionalMongoClients = await Program.DiscoverAvailableRegionsAsync(mongoClient);

                // Write documents to all available regions
                foreach (KeyValuePair<string, MongoClient> regionalMongoClient in regionalMongoClients)
                {
                    await Program.ExecuteWriteWorkloadAsync(regionalMongoClient.Value, Guid.NewGuid());
                }

                // If there are multiple regions, generate and examine conflicts
                if (regionalMongoClients.Count > 1)
                {
                    await Program.ExecuteConflictsWorkloadAsync(regionalMongoClients.Values.ToList(), Guid.NewGuid());
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Console.WriteLine(Program.HelpText);
            }

            Console.WriteLine();
            Console.WriteLine("Press any key...");
            Console.ReadLine();
        }

        private static async Task<Dictionary<string, MongoClient>> DiscoverAvailableRegionsAsync(MongoClient mongoClient)
        {
            Console.WriteLine("Discovering all available regions...");
            Console.WriteLine();

            KeyValuePair<string, TimeSpan>[] availableRegions = mongoClient.Cluster.Description.Servers
                .Select(x => new KeyValuePair<string, TimeSpan>(Helpers.TryGetRegionFromTags(x.Tags), x.AverageRoundTripTime))
                .ToArray();

            Console.WriteLine($"{mongoClient.Cluster.Description.Servers.Count} regions available:");

            int maxRegionCharacters = availableRegions.Max(x => x.Key.Length);

            foreach (KeyValuePair<string, TimeSpan> availableRegion in availableRegions.OrderBy(x => x.Value))
            {
                Console.WriteLine($"  {availableRegion.Key.PadRight(maxRegionCharacters)} [{availableRegion.Value.TotalMilliseconds} milliseconds round-trip latency]");
            }

            Console.WriteLine();

            Dictionary<string, MongoClient> regionToMongoClient = new Dictionary<string, MongoClient>();

            foreach (KeyValuePair<string, TimeSpan> availableRegion in availableRegions)
            {
                MongoClient regionMongoClient = Helpers.GetMongoClientWithPreferredWriteRegion(mongoClient.Settings, availableRegion.Key);
                string actualWriteRegion = await Program.WaitForConnectionsToAllServersAsync(regionMongoClient, TimeSpan.FromSeconds(30));
                regionToMongoClient[actualWriteRegion] = regionMongoClient;

                if (!string.Equals(actualWriteRegion, availableRegion.Key, StringComparison.Ordinal))
                {
                    Console.WriteLine("Actual write region differs from preferred write region (is multi-master enabled for this account?).");
                }
            }

            Console.WriteLine();

            return regionToMongoClient;
        }

        /// <summary>
        /// The write workload writes a set of documents
        /// </summary>
        private static async Task ExecuteWriteWorkloadAsync(MongoClient mongoClient, Guid runGuid)
        {
            IMongoDatabase mongoDatabase = mongoClient.GetDatabase(ConfigurationManager.AppSettings["database"]);
            IMongoCollection<BsonDocument> mongoCollection = mongoDatabase.GetCollection<BsonDocument>(ConfigurationManager.AppSettings["collection"]);

            ServerDescription primaryServerDescription = mongoClient.Cluster.Description.Servers.First(x => x.Type == ServerType.ReplicaSetPrimary);
            string region = Helpers.TryGetRegionFromTags(primaryServerDescription.Tags) ?? string.Empty;

            BsonDocument template = new BsonDocument(new BsonElement("writtenFrom", new BsonString(region)));

            const int NumToWrite = 100;

            Console.WriteLine($"Writing {NumToWrite} documents to {region}...");

            Stopwatch stopWatch = Stopwatch.StartNew();

            for (int i = 0; i < NumToWrite; ++i)
            {
                BsonDocument toInsert = (BsonDocument)template.DeepClone();
                toInsert["_id"] = new BsonString($"{runGuid}:{i}");
                await mongoCollection.InsertOneAsync(toInsert);
            }

            Console.WriteLine($"Complete ({stopWatch.ElapsedMilliseconds} milliseconds).");
            Console.WriteLine();
        }

        /// <summary>
        /// The conflicts workload writes to all regions concurrently until a certain number of conflicts have been examined.
        /// </summary>
        private static async Task ExecuteConflictsWorkloadAsync(IReadOnlyList<MongoClient> mongoClients, Guid runGuid)
        {
            int documentsRemaining = 1000;
            int conflictsRemaining = 3;
            int id = 0;

            Console.WriteLine($"Writing up to {documentsRemaining} documents to all regions to generate and examine up to {conflictsRemaining} conflicts...");

            while (conflictsRemaining > 0 && documentsRemaining > 0)
            {
                documentsRemaining--;
                string currentId = $"{runGuid}:{id}";

                Task<BsonDocument>[] writeTasks = mongoClients
                    .Select(async mongoClient =>
                        {
                            IMongoDatabase mongoDatabase = mongoClient.GetDatabase(ConfigurationManager.AppSettings["database"]);
                            IMongoCollection<BsonDocument> mongoCollection = mongoDatabase.GetCollection<BsonDocument>(ConfigurationManager.AppSettings["collection"]);

                            ServerDescription primaryServerDescription = mongoClient.Cluster.Description.Servers.First(x => x.Type == ServerType.ReplicaSetPrimary);
                            string region = Helpers.TryGetRegionFromTags(primaryServerDescription.Tags) ?? string.Empty;

                            BsonDocument toInsert = new BsonDocument(
                                (IEnumerable<BsonElement>)new[]
                                    {
                                        new BsonElement("_id", new BsonString(currentId)),
                                        new BsonElement("writtenFrom", new BsonString(region)),
                                    });

                            await Task.Yield();
                            await mongoCollection.InsertOneAsync(toInsert);
                            return toInsert;
                        })
                    .ToArray();

                try
                {
                    await Task.WhenAll(writeTasks);
                }
                catch (MongoWriteException)
                {
                    // When a conflict does not occur, one write will fail with a duplicate _id error
                    // This exception is expected, unless a conflict occurred.
                    // In a conflict scenario, writes will succeed to multiple regions even with the
                    // same _id, and the winning write will be chosen with conflict resolution.
                }

                Task<BsonDocument>[] completedSuccessfully = writeTasks.Where(x => x.Status == TaskStatus.RanToCompletion).ToArray();

                if (completedSuccessfully.Length > 1)
                {
                    await Task.Delay(1000);

                    Console.WriteLine();
                    Console.WriteLine("Conflict happened! These documents all written successfully to different regions: ");
                    foreach (Task<BsonDocument> success in completedSuccessfully)
                    {
#pragma warning disable AsyncFixer02 // Long running or blocking operations under an async method
                        Console.WriteLine("  " + success.Result.ToString());
#pragma warning restore AsyncFixer02 // Long running or blocking operations under an async method
                    }

                    // Conflict resolution is based on the server-side timestamp of the write
                    // In a conflict scenario, the document with the latest timestamp wins
                    // The timestamp depends on the server-side clock in each region and as is not
                    // currently exposed through the MongoDB API
                    Console.WriteLine("Winner: ");

                    IMongoDatabase mongoDatabase = mongoClients[0].GetDatabase(ConfigurationManager.AppSettings["database"]);
                    IMongoCollection<BsonDocument> mongoCollection = mongoDatabase.GetCollection<BsonDocument>(ConfigurationManager.AppSettings["collection"]);

                    BsonDocument winner = (await mongoCollection.Find(x => x["_id"] == currentId).ToListAsync())[0];
                    Console.WriteLine("  " + winner.ToString());
                    conflictsRemaining--;
                }

                id++;
            }

            Console.WriteLine("Complete.");
        }

        /// <summary>
        /// Wait for connections to be established to all replica set members
        /// </summary>
        private static async Task<string> WaitForConnectionsToAllServersAsync(MongoClient mongoClient, TimeSpan timeout)
        {
            await mongoClient.ListDatabasesAsync();

            Stopwatch stopwatch = Stopwatch.StartNew();
            while (mongoClient.Cluster.Description.Servers.Any(x => x.State != ServerState.Connected))
            {
                if (stopwatch.ElapsedMilliseconds > timeout.TotalMilliseconds)
                {
                    throw new InvalidOperationException("Timed out waiting for connections to all replica set members.");
                }

                await Task.Delay(1000);
            }

            ServerDescription primaryServerDescription = mongoClient.Cluster.Description.Servers.First(x => x.Type == ServerType.ReplicaSetPrimary);
            string region = Helpers.TryGetRegionFromTags(primaryServerDescription.Tags) ?? string.Empty;

            string verifyPrimaryRegion = Program.GetPreferredWriteRegionFromApplicationName(mongoClient.Settings.ApplicationName);
            Console.WriteLine($"Requested primary region {verifyPrimaryRegion ?? "<default>"}; received primary region {region}.");
            return region;
        }

        /// <summary>
        /// Extract the preferred write region from the application name
        /// </summary>
        private static string GetPreferredWriteRegionFromApplicationName(string applicationName)
        {
            if (applicationName == null)
            {
                return null;
            }

            int lastIndexOfAmpersand = applicationName.LastIndexOf('@');
            string preferredWriteRegion = applicationName.Substring(lastIndexOfAmpersand + 1);
            return preferredWriteRegion;
        }
    }
}
