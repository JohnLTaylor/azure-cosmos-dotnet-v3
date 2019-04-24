﻿//-----------------------------------------------------------------------
// <copyright file="CrossPartitionQueryTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace Microsoft.Azure.Cosmos.SDK.EmulatorTests
{
    using System;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.Diagnostics;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Xml;
    using Microsoft.Azure.Cosmos.CosmosElements;
    using Microsoft.Azure.Cosmos.Linq;
    using Microsoft.Azure.Cosmos.Routing;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Routing;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Converters;
    using Newtonsoft.Json.Linq;
    using Query;
    using Query.ParallelQuery;

    /// <summary>
    /// Tests for CrossPartitionQueryTests.
    /// </summary>
    [TestClass]
    public class CrossPartitionQueryTests
    {
        private static readonly string[] NoDocuments = new string[] { };
        private CosmosClient GatewayClient = TestCommon.CreateCosmosClient(true);
        private CosmosClient Client = TestCommon.CreateCosmosClient(false);
        private CosmosDatabase database;
        private readonly AsyncLocal<LocalCounter> responseLengthBytes = new AsyncLocal<LocalCounter>();
        private readonly AsyncLocal<Guid> outerCosmosQueryResponseActivityId = new AsyncLocal<Guid>();

        [FlagsAttribute]
        private enum ConnectionModes
        {
            None = 0,
            Direct = 0x1,
            Gateway = 0x2,
        }

        [TestInitialize]
        public async Task Initialize()
        {
            await this.CleanUp();
            this.database = await this.Client.Databases.CreateDatabaseAsync(Guid.NewGuid().ToString() + "db");
        }

        [TestCleanup]
        public async Task Cleanup()
        {
            await this.database.DeleteAsync();
        }

        private static string GetApiVersion()
        {
            return HttpConstants.Versions.CurrentVersion;
        }

        private static void SetApiVersion(string apiVersion)
        {
            HttpConstants.Versions.CurrentVersion = apiVersion;
            HttpConstants.Versions.CurrentVersionUTF8 = Encoding.UTF8.GetBytes(apiVersion);
        }

        private async Task<IReadOnlyList<PartitionKeyRange>> GetPartitionKeyRanges(CosmosContainerSettings container)
        {
            Range<string> fullRange = new Range<string>(
                PartitionKeyInternal.MinimumInclusiveEffectivePartitionKey,
                PartitionKeyInternal.MaximumExclusiveEffectivePartitionKey,
                true,
                false);
            IRoutingMapProvider routingMapProvider = await this.Client.DocumentClient.GetPartitionKeyRangeCacheAsync();
            IReadOnlyList<PartitionKeyRange> ranges = await routingMapProvider.TryGetOverlappingRangesAsync(container.ResourceId, fullRange);
            return ranges;
        }

        private async Task<CosmosContainer> CreatePartitionContainer(string partitionKey = "/id", Microsoft.Azure.Cosmos.IndexingPolicy indexingPolicy = null)
        {
            CosmosContainerResponse containerResponse = await this.database.Containers.CreateContainerAsync(
                new CosmosContainerSettings
                {
                    Id = Guid.NewGuid().ToString() + "container",
                    IndexingPolicy = indexingPolicy == null ? new Cosmos.IndexingPolicy
                    {
                        IncludedPaths = new Collection<Cosmos.IncludedPath>
                        {
                            new Cosmos.IncludedPath
                            {
                                Path = "/*",
                                Indexes = new Collection<Cosmos.Index>
                                {
                                    Cosmos.Index.Range(Cosmos.DataType.Number),
                                    Cosmos.Index.Range(Cosmos.DataType.String),
                                }
                            }
                        }
                    } : indexingPolicy,
                    PartitionKey = new PartitionKeyDefinition
                    {
                        Paths = new Collection<string> { partitionKey },
                        Kind = PartitionKind.Hash
                    }
                },
                // This throughput needs to be about half the max with multi master
                // otherwise it will create about twice as many partitions.
                25000);

            IReadOnlyList<PartitionKeyRange> ranges = await this.GetPartitionKeyRanges(containerResponse);
            Assert.AreEqual(5, ranges.Count());

            return containerResponse;
        }

        private async Task<Tuple<CosmosContainer, List<Document>>> CreatePartitionedContainerAndIngestDocuments(IEnumerable<string> documents, string partitionKey = "/id", Cosmos.IndexingPolicy indexingPolicy = null)
        {
            CosmosContainer partitionedCollection = await this.CreatePartitionContainer(partitionKey, indexingPolicy);
            List<Document> insertedDocuments = new List<Document>();
            string jObjectPartitionKey = partitionKey.Remove(0, 1);
            foreach (string document in documents)
            {
                JObject documentObject = JsonConvert.DeserializeObject<JObject>(document);
                if (documentObject["id"] == null)
                {
                    documentObject["id"] = Guid.NewGuid().ToString();
                }

                JValue pkToken = (JValue)documentObject[jObjectPartitionKey];
                object pkValue = pkToken != null ? pkToken.Value : Undefined.Value;
                insertedDocuments.Add((await partitionedCollection.Items.CreateItemAsync<JObject>(pkValue, documentObject)).Resource.ToObject<Document>());

            }

            return new Tuple<CosmosContainer, List<Document>>(partitionedCollection, insertedDocuments);
        }

        private async Task CleanUp()
        {
            CosmosResultSetIterator<CosmosDatabaseSettings> allDatabases = this.Client.Databases.GetDatabaseIterator();

            while (allDatabases.HasMoreResults)
            {
                foreach (CosmosDatabaseSettings db in await allDatabases.FetchNextSetAsync())
                {
                    await this.Client.Databases[db.Id].DeleteAsync();
                }
            }
        }

        private async Task RunWithApiVersion(string apiVersion, Func<Task> function)
        {
            string originalApiVersion = GetApiVersion();
            CosmosClient originalCosmosClient = this.Client;
            CosmosClient originalGatewayClient = this.GatewayClient;
            CosmosDatabase originalDatabase = this.database;

            try
            {
                SetApiVersion(apiVersion);
                if (apiVersion != originalApiVersion)
                {
                    this.Client = TestCommon.CreateCosmosClient(false);
                    this.GatewayClient = TestCommon.CreateCosmosClient(true);
                    this.database = this.Client.Databases[this.database.Id];
                }

                await function();
            }
            finally
            {
                this.Client = originalCosmosClient;
                this.GatewayClient = originalGatewayClient;
                this.database = originalDatabase;
                SetApiVersion(originalApiVersion);
            }
        }

        internal delegate Task Query(
            CosmosContainer container,
            IEnumerable<Document> documents);

        internal delegate Task Query<T>(
            CosmosContainer container,
            IEnumerable<Document> documents,
            T testArgs);

        internal delegate CosmosClient CosmosClientFactory(ConnectionMode connectionMode);

        private async Task CreateIngestQueryDelete(
            ConnectionModes connectionModes,
            IEnumerable<string> documents,
            Query query,
            string partitionKey = "/id",
            Cosmos.IndexingPolicy indexingPolicy = null,
            CosmosClientFactory cosmosClientFactory = null)
        {
            Query<object> queryWrapper = (container, inputDocuments, throwaway) =>
            {
                return query(container, inputDocuments);
            };

            await this.CreateIngestQueryDelete<object>(
                connectionModes,
                documents,
                queryWrapper,
                null,
                partitionKey,
                indexingPolicy,
                cosmosClientFactory);
        }

        private async Task CreateIngestQueryDelete<T>(
            ConnectionModes connectionModes,
            IEnumerable<string> documents,
            Query<T> query,
            T testArgs,
            string partitionKey = "/id",
            Cosmos.IndexingPolicy indexingPolicy = null,
            CosmosClientFactory cosmosClientFactory = null)
        {
            await this.CreateIngestQueryDelete(
                connectionModes,
                documents,
                query,
                cosmosClientFactory ?? this.CreateDefaultCosmosClient,
                testArgs,
                partitionKey,
                indexingPolicy);
        }

        /// <summary>
        /// Task that wraps boiler plate code for query tests (container create -> ingest documents -> query documents -> delete collections).
        /// Note that this function will take the cross product connectionModes
        /// </summary>
        /// <param name="connectionModes">The connection modes to use.</param>
        /// <param name="documents">The documents to ingest</param>
        /// <param name="query">
        /// The callback for the queries.
        /// All the standard arguments will be passed in.
        /// Please make sure that this function is idempotent, since a container will be reused for each connection mode.
        /// </param>
        /// <param name="cosmosClientFactory">
        /// The callback for the create CosmosClient. This is invoked for the different ConnectionModes that the query is targeting.
        /// If CosmosClient instantiated by this does not apply the expected ConnectionMode, an assert is thrown.
        /// </param>
        /// <param name="partitionKey">The partition key for the partition container.</param>
        /// <param name="testArgs">The optional args that you want passed in to the query.</param>
        /// <returns>A task to await on.</returns>
        private async Task CreateIngestQueryDelete<T>(
           ConnectionModes connectionModes,
           IEnumerable<string> documents,
           Query<T> query,
           CosmosClientFactory cosmosClientFactory,
           T testArgs,
           string partitionKey = "/id",
           Cosmos.IndexingPolicy indexingPolicy = null)
        {
            int retryCount = 1;
            AggregateException exceptionHistory = new AggregateException();
            while (retryCount-- > 0)
            {
                try
                {
                    List<Task<Tuple<CosmosContainer, List<Document>>>> createContainerTasks = new List<Task<Tuple<CosmosContainer, List<Document>>>>
                    {
                        this.CreatePartitionedContainerAndIngestDocuments(documents, partitionKey, indexingPolicy)
                    };

                    Tuple<CosmosContainer, List<Document>>[] collectionsAndDocuments = await Task.WhenAll(createContainerTasks);

                    List<CosmosClient> cosmosClients = new List<CosmosClient>();
                    foreach (ConnectionModes connectionMode in Enum.GetValues(connectionModes.GetType()).Cast<Enum>().Where(connectionModes.HasFlag))
                    {
                        if (connectionMode == ConnectionModes.None)
                        {
                            continue;
                        }

                        ConnectionMode targetConnectionMode = GetTargetConnectionMode(connectionMode);
                        CosmosClient cosmosClient = cosmosClientFactory(targetConnectionMode);

                        Assert.AreEqual(targetConnectionMode, cosmosClient.Configuration.ConnectionMode, "Test setup: Invalid connection policy applied to CosmosClient");
                        cosmosClients.Add(cosmosClient);
                    }

                    bool succeeded = false;
                    while (!succeeded)
                    {
                        try
                        {
                            List<Task> queryTasks = new List<Task>();
                            foreach (CosmosClient cosmosClient in cosmosClients)
                            {
                                foreach (Tuple<CosmosContainer, List<Document>> containerAndDocuments in collectionsAndDocuments)
                                {
                                    CosmosContainer container = cosmosClient.Databases[containerAndDocuments.Item1.Database.Id].Containers[containerAndDocuments.Item1.Id];
                                    queryTasks.Add(query(container, containerAndDocuments.Item2, testArgs));
                                }
                            }

                            await Task.WhenAll(queryTasks);
                            succeeded = true;
                        }
                        catch (TaskCanceledException)
                        {
                            // SDK throws TaskCanceledException every now and then
                        }
                    }

                    List<Task<CosmosContainerResponse>> deleteContainerTasks = new List<Task<CosmosContainerResponse>>();
                    foreach (CosmosContainer container in collectionsAndDocuments.Select(tuple => tuple.Item1))
                    {
                        deleteContainerTasks.Add(container.DeleteAsync());
                    }

                    await Task.WhenAll(deleteContainerTasks);

                    // If you made it here then it's all good
                    break;
                }
                catch (Exception ex)
                {
                    if (ex.GetType() == typeof(AssertFailedException))
                    {
                        throw;
                    }
                    else
                    {
                        List<Exception> previousExceptions = exceptionHistory.InnerExceptions.ToList();
                        previousExceptions.Add(ex);
                        exceptionHistory = new AggregateException(previousExceptions);
                    }
                }
            }

            if (exceptionHistory.InnerExceptions.Count > 0)
            {
                throw exceptionHistory;
            }
        }

        private static ConnectionMode GetTargetConnectionMode(ConnectionModes connectionMode)
        {
            ConnectionMode targetConnectionMode = ConnectionMode.Gateway;
            switch (connectionMode)
            {
                case ConnectionModes.Gateway:
                    targetConnectionMode = ConnectionMode.Gateway;
                    break;

                case ConnectionModes.Direct:
                    targetConnectionMode = ConnectionMode.Direct;
                    break;

                default:
                    throw new ArgumentException($"Unexpected connection mode: {connectionMode}");
            }

            return targetConnectionMode;
        }

        private CosmosClient CreateDefaultCosmosClient(ConnectionMode connectionMode)
        {
            switch (connectionMode)
            {
                case ConnectionMode.Gateway:
                    return this.GatewayClient;
                case ConnectionMode.Direct:
                    return this.Client;
                default:
                    throw new ArgumentException($"Unexpected connection mode: {connectionMode}");
            }
        }

        private CosmosClient CreateNewCosmosClient(ConnectionMode connectionMode)
        {
            switch (connectionMode)
            {
                case ConnectionMode.Gateway:
                    return TestCommon.CreateCosmosClient(true);
                case ConnectionMode.Direct:
                    return TestCommon.CreateCosmosClient(false);
                default:
                    throw new ArgumentException($"Unexpected connection mode: {connectionMode}");
            }
        }

        private static async Task<List<T>> QueryWithoutContinuationTokens<T>(
            CosmosContainer container,
            string query,
            int maxItemCount,
            CosmosQueryRequestOptions queryRequestOptions = null)
        {
            List<T> results = new List<T>();
            CosmosResultSetIterator<T> itemQuery = container.Items.CreateItemQuery<T>(
                sqlQueryText: query,
                maxConcurrency: 2,
                requestOptions: queryRequestOptions);

            while (itemQuery.HasMoreResults)
            {
                results.AddRange(await itemQuery.FetchNextSetAsync());
            }

            return results;
        }

        private static async Task NoOp()
        {
            await Task.Delay(0);
        }

        private async Task RandomlyThrowException(Exception exception = null)
        {
            await CrossPartitionQueryTests.NoOp();
            Random random = new Random();
            if (random.Next(0, 2) == 0)
            {
                throw exception;
            }
        }

        [TestMethod]
        public async Task TestBadQueriesOverMultiplePartitions()
        {
            await this.CreateIngestQueryDelete(
                ConnectionModes.Direct | ConnectionModes.Gateway,
                CrossPartitionQueryTests.NoDocuments,
                this.TestBadQueriesOverMultiplePartitionsHelper);
        }

        [TestMethod]
        public void TestContinuationTokenSerialization()
        {
            CompositeContinuationToken compositeContinuationToken = new CompositeContinuationToken()
            {
                Token = "asdf",
                Range = new Range<string>("asdf", "asdf", false, false),
            };

            string serializedCompositeContinuationToken = JsonConvert.SerializeObject(compositeContinuationToken);
            CompositeContinuationToken deserializedCompositeContinuationToken = JsonConvert.DeserializeObject<CompositeContinuationToken>(serializedCompositeContinuationToken);
            Assert.AreEqual(compositeContinuationToken.Token, deserializedCompositeContinuationToken.Token);
            //Assert.IsTrue(compositeContinuationToken.Range.Equals(deserializedCompositeContinuationToken.Range));


            string orderByItemSerialized = @"{""item"" : 1337 }";
            byte[] bytes = Encoding.UTF8.GetBytes(orderByItemSerialized);
            OrderByItem orderByItem = new OrderByItem(CosmosElement.Create(bytes));
            OrderByContinuationToken orderByContinuationToken = new OrderByContinuationToken(
                compositeContinuationToken,
                new List<OrderByItem> { orderByItem },
                "asdf",
                42,
                "asdf");
            string serializedOrderByContinuationToken = JsonConvert.SerializeObject(orderByContinuationToken);
            OrderByContinuationToken deserializedOrderByContinuationToken = JsonConvert.DeserializeObject<OrderByContinuationToken>(serializedOrderByContinuationToken);
            Assert.AreEqual(
                orderByContinuationToken.CompositeContinuationToken.Token,
                deserializedOrderByContinuationToken.CompositeContinuationToken.Token);
            //Assert.IsTrue(
            //    orderByContinuationToken.CompositeContinuationToken.Range.Equals(
            //    deserializedOrderByContinuationToken.CompositeContinuationToken.Range));
            Assert.IsTrue(CosmosElementEqualityComparer.Value.Equals(orderByContinuationToken.OrderByItems[0].Item, deserializedOrderByContinuationToken.OrderByItems[0].Item));
            Assert.AreEqual(orderByContinuationToken.Rid, deserializedOrderByContinuationToken.Rid);
            Assert.AreEqual(orderByContinuationToken.SkipCount, deserializedOrderByContinuationToken.SkipCount);
        }

        private async Task TestBadQueriesOverMultiplePartitionsHelper(CosmosContainer container, IEnumerable<Document> documents)
        {
            await CrossPartitionQueryTests.NoOp();
            try
            {
                CosmosResultSetIterator<Document> resultSetIterator = container.Items.CreateItemQuery<Document>(
                    @"SELECT * FROM Root r WHERE a = 1",
                    maxConcurrency: 2);

                await resultSetIterator.FetchNextSetAsync();

                Assert.Fail("Expected CosmosException");
            }
            catch (AggregateException e)
            {
                CosmosException exception = e.InnerException as CosmosException;

                if (exception == null)
                {
                    throw e;
                }

                if (exception.StatusCode != HttpStatusCode.BadRequest)
                {
                    throw e;
                }

                if (!exception.Message.StartsWith("Response status code does not indicate success: 400 Substatus: 0 Reason: (Message: {\"errors\":[{\"severity\":\"Error\",\"location\":{\"start\":27,\"end\":28},\"code\":\"SC2001\",\"message\":\"Identifier 'a' could not be resolved.\"}]}"))
                {
                    throw e;
                }
            }
        }

        /// <summary>
        //"SELECT c._ts, c.id, c.TicketNumber, c.PosCustomerNumber, c.CustomerId, c.CustomerUserId, c.ContactEmail, c.ContactPhone, c.StoreCode, c.StoreUid, c.PoNumber, c.OrderPlacedOn, c.OrderType, c.OrderStatus, c.Customer.UserFirstName, c.Customer.UserLastName, c.Customer.Name, c.UpdatedBy, c.UpdatedOn, c.ExpirationDate, c.TotalAmountFROM c ORDER BY c._ts"' created an ArgumentOutofRangeException since ServiceInterop was returning DISP_E_BUFFERTOOSMALL in the case of an invalid query that is also really long.
        /// This test case just double checks that you get the appropriate document client exception instead of just failing.
        /// </summary>
        [Ignore]
        [TestCategory("Quarantine")] //until serviceInterop enabled again
        [TestMethod]
        public async Task TestQueryCrossParitionPartitionProviderInvalid()
        {
            await this.CreateIngestQueryDelete(
                ConnectionModes.Direct | ConnectionModes.Gateway,
                CrossPartitionQueryTests.NoDocuments,
                this.TestQueryCrossParitionPartitionProviderInvalidHelper);
        }

        private async Task TestQueryCrossParitionPartitionProviderInvalidHelper(CosmosContainer container, IEnumerable<Document> documents)
        {
            await CrossPartitionQueryTests.NoOp();
            try
            {
                /// note that there is no space before the from clause thus this query should fail 
                /// '"code":"SC2001","message":"Identifier 'c' could not be resolved."'
                string query = "SELECT c._ts, c.id, c.TicketNumber, c.PosCustomerNumber, c.CustomerId, c.CustomerUserId, c.ContactEmail, c.ContactPhone, c.StoreCode, c.StoreUid, c.PoNumber, c.OrderPlacedOn, c.OrderType, c.OrderStatus, c.Customer.UserFirstName, c.Customer.UserLastName, c.Customer.Name, c.UpdatedBy, c.UpdatedOn, c.ExpirationDate, c.TotalAmountFROM c ORDER BY c._ts";
                List<Document> expectedValues = new List<Document>();
                CosmosResultSetIterator<Document> resultSetIterator = container.Items.CreateItemQuery<Document>(
                    query,
                    maxConcurrency: 0);

                while (resultSetIterator.HasMoreResults)
                {
                    expectedValues.AddRange(await resultSetIterator.FetchNextSetAsync());
                }

                Assert.Fail("Expected to get an exception for this query.");
            }
            catch (AggregateException e)
            {
                bool gotBadRequest = false;
                foreach (Exception inner in e.InnerExceptions)
                {
                    if (inner is CosmosException dce)
                    {
                        if (dce.StatusCode == HttpStatusCode.BadRequest)
                        {
                            gotBadRequest = true;
                        }
                    }
                }

                Assert.IsTrue(gotBadRequest);
            }
        }

        [TestMethod]
        public async Task TestQueryAndReadFeedWithPartitionKey()
        {
            string[] documents = new[]
            {
                @"{""id"":""documentId1"",""key"":""A"",""prop"":3,""shortArray"":[{""a"":5}]}",
                @"{""id"":""documentId2"",""key"":""A"",""prop"":2,""shortArray"":[{""a"":6}]}",
                @"{""id"":""documentId3"",""key"":""A"",""prop"":1,""shortArray"":[{""a"":7}]}",
                @"{""id"":""documentId4"",""key"":5,""prop"":3,""shortArray"":[{""a"":5}]}",
                @"{""id"":""documentId5"",""key"":5,""prop"":2,""shortArray"":[{""a"":6}]}",
                @"{""id"":""documentId6"",""key"":5,""prop"":1,""shortArray"":[{""a"":7}]}",
                @"{""id"":""documentId10"",""prop"":3,""shortArray"":[{""a"":5}]}",
                @"{""id"":""documentId11"",""prop"":2,""shortArray"":[{""a"":6}]}",
                @"{""id"":""documentId12"",""prop"":1,""shortArray"":[{""a"":7}]}",
            };

            await this.CreateIngestQueryDelete(
                ConnectionModes.Direct | ConnectionModes.Gateway,
                documents,
                this.TestQueryAndReadFeedWithPartitionKeyHelper,
                "/key");
        }

        private async Task TestQueryAndReadFeedWithPartitionKeyHelper(
            CosmosContainer container,
            IEnumerable<Document> documents)
        {
            Assert.AreEqual(0, (await this.RunQuery<Document>(
                container,
                @"SELECT * FROM Root r WHERE false",
                maxConcurrency: 1)).Count);

            object[] keys = new object[] { "A", 5, Undefined.Value };
            for (int i = 0; i < keys.Length; ++i)
            {
                List<string> expected = documents.Skip(i * 3).Take(3).Select(doc => doc.Id).ToList();
                string expectedResult = string.Join(",", expected);
                // Order-by
                expected.Reverse();
                string expectedOrderByResult = string.Join(",", expected);

                List<(string, string)> queries = new List<(string, string)>()
                {
                    ($@"SELECT * FROM Root r WHERE r.id IN (""{expected[0]}"", ""{expected[1]}"", ""{expected[2]}"")", expectedResult),
                    (@"SELECT * FROM Root r WHERE r.prop BETWEEN 1 AND 3", expectedResult),
                    (@"SELECT VALUE r FROM Root r JOIN c IN r.shortArray WHERE c.a BETWEEN 5 and 7", expectedResult),
                    ($@"SELECT TOP 10 * FROM Root r WHERE r.id IN (""{expected[0]}"", ""{expected[1]}"", ""{expected[2]}"")", expectedResult),
                    (@"SELECT TOP 10 * FROM Root r WHERE r.prop BETWEEN 1 AND 3", expectedResult),
                    (@"SELECT TOP 10 VALUE r FROM Root r JOIN c IN r.shortArray WHERE c.a BETWEEN 5 and 7", expectedResult),
                    ($@"SELECT * FROM Root r WHERE r.id IN (""{expected[0]}"", ""{expected[1]}"", ""{expected[2]}"") ORDER BY r.prop", expectedOrderByResult),
                    (@"SELECT * FROM Root r WHERE r.prop BETWEEN 1 AND 3 ORDER BY r.prop", expectedOrderByResult),
                    (@"SELECT VALUE r FROM Root r JOIN c IN r.shortArray WHERE c.a BETWEEN 5 and 7 ORDER BY r.prop", expectedOrderByResult),
                };



                if (i < keys.Length - 1)
                {
                    string key;
                    if (keys[i] is string)
                    {
                        key = "'" + keys[i].ToString() + "'";
                    }
                    else
                    {
                        key = keys[i].ToString();
                    }

                    queries.Add((string.Format(CultureInfo.InvariantCulture, @"SELECT * FROM Root r WHERE r.key = {0} ORDER BY r.prop", key), expectedOrderByResult));
                }

                foreach ((string, string) queryAndExpectedResult in queries)
                {
                    CosmosResultSetIterator<Document> resultSetIterator = container.Items.CreateItemQuery<Document>(
                        sqlQueryText: queryAndExpectedResult.Item1,
                        partitionKey: keys[i],
                        maxItemCount: 1);

                    List<Document> result = new List<Document>();
                    while (resultSetIterator.HasMoreResults)
                    {
                        result.AddRange(await resultSetIterator.FetchNextSetAsync());
                    }

                    string resultDocIds = string.Join(",", result.Select(doc => doc.Id));
                    Assert.AreEqual(queryAndExpectedResult.Item2, resultDocIds);
                }
            }
        }

        [TestMethod]
        public async Task TestQueryMultiplePartitionsSinglePartitionKey()
        {
            string[] documents = new[]
            {
                @"{""pk"":""doc1""}",
                @"{""pk"":""doc2""}",
                @"{""pk"":""doc3""}",
                @"{""pk"":""doc4""}",
                @"{""pk"":""doc5""}",
                @"{""pk"":""doc6""}",
            };

            await this.CreateIngestQueryDelete(
                ConnectionModes.Direct | ConnectionModes.Gateway,
                documents,
                this.TestQueryMultiplePartitionsSinglePartitionKeyHelper,
                "/pk");
        }

        private async Task TestQueryMultiplePartitionsSinglePartitionKeyHelper(CosmosContainer container, IEnumerable<Document> documents)
        {
            // Query with partition key should be done in one round trip.
            CosmosResultSetIterator<dynamic> resultSetIterator = container.Items.CreateItemQuery<dynamic>(
                "SELECT * FROM c WHERE c.pk = 'doc5'",
                partitionKey: "doc5");

            CosmosQueryResponse<dynamic> response = await resultSetIterator.FetchNextSetAsync();
            Assert.AreEqual(1, response.Count());
            Assert.IsNull(response.ContinuationToken);

            resultSetIterator = container.Items.CreateItemQuery<dynamic>(
               "SELECT * FROM c WHERE c.pk = 'doc10'",
               partitionKey: "doc10");

            response = await resultSetIterator.FetchNextSetAsync();
            Assert.AreEqual(0, response.Count());
            Assert.IsNull(response.ContinuationToken);
        }

        private struct QueryWithSpecialPartitionKeysArgs
        {
            public string Name;
            public object Value;
            public Func<object, object> ValueToPartitionKey;
        }

        // V3 only supports Numeric, string, bool, null, undefined
        [TestMethod]
        [Ignore]
        public async Task TestQueryWithSpecialPartitionKeys()
        {
            await CrossPartitionQueryTests.NoOp();
            QueryWithSpecialPartitionKeysArgs[] queryWithSpecialPartitionKeyArgsList = new QueryWithSpecialPartitionKeysArgs[]
            {
                new QueryWithSpecialPartitionKeysArgs()
                {
                    Name = "Guid",
                    Value = Guid.NewGuid(),
                    ValueToPartitionKey = val => val.ToString(),
                },
                //new QueryWithSpecialPartitionKeysArgs()
                //{
                //    Name = "DateTime",
                //    Value = DateTime.Now,
                //    ValueToPartitionKey = val =>
                //    {
                //        string str = JsonConvert.SerializeObject(
                //            val,
                //            new JsonSerializerSettings()
                //            {
                //                Converters = new List<JsonConverter> { new IsoDateTimeConverter() }
                //            });
                //        return str.Substring(1, str.Length - 2);
                //    },
                //},
                new QueryWithSpecialPartitionKeysArgs()
                {
                    Name = "Enum",
                    Value = HttpStatusCode.OK,
                    ValueToPartitionKey = val => (int)val,
                },
                new QueryWithSpecialPartitionKeysArgs()
                {
                    Name = "CustomEnum",
                    Value = HttpStatusCode.OK,
                    ValueToPartitionKey = val => val.ToString(),
                },
                new QueryWithSpecialPartitionKeysArgs()
                {
                    Name = "ResourceId",
                    Value = "testid",
                    ValueToPartitionKey = val => val,
                },
                new QueryWithSpecialPartitionKeysArgs()
                {
                    Name = "CustomDateTime",
                    Value = new DateTime(2016, 11, 12),
                    ValueToPartitionKey = val => EpochDateTimeConverter.DateTimeToEpoch((DateTime)val),
                },
            };

            foreach (QueryWithSpecialPartitionKeysArgs testArg in queryWithSpecialPartitionKeyArgsList)
            {
                // For this test we need to split direct and gateway runs into separate collections,
                // since the query callback inserts some documents (thus has side effects).
                await this.CreateIngestQueryDelete<QueryWithSpecialPartitionKeysArgs>(
                    ConnectionModes.Direct,
                    CrossPartitionQueryTests.NoDocuments,
                    this.TestQueryWithSpecialPartitionKeysHelper,
                    testArg,
                    "/" + testArg.Name);

                await this.CreateIngestQueryDelete<QueryWithSpecialPartitionKeysArgs>(
                    ConnectionModes.Gateway,
                    CrossPartitionQueryTests.NoDocuments,
                    this.TestQueryWithSpecialPartitionKeysHelper,
                    testArg,
                    "/" + testArg.Name);
            }
        }

        private async Task TestQueryWithSpecialPartitionKeysHelper(CosmosContainer container, IEnumerable<Document> documents, QueryWithSpecialPartitionKeysArgs testArgs)
        {
            QueryWithSpecialPartitionKeysArgs args = testArgs;

            SpecialPropertyDocument specialPropertyDocument = new SpecialPropertyDocument
            {
                id = Guid.NewGuid().ToString()
            };

            specialPropertyDocument.GetType().GetProperty(args.Name).SetValue(specialPropertyDocument, args.Value);
            Func<SpecialPropertyDocument, object> getPropertyValueFunction = d => d.GetType().GetProperty(args.Name).GetValue(d);

            CosmosItemResponse<SpecialPropertyDocument> response = await container.Items.CreateItemAsync<SpecialPropertyDocument>(testArgs.Value, specialPropertyDocument);
            dynamic returnedDoc = response.Resource;
            Assert.AreEqual(args.Value, getPropertyValueFunction((SpecialPropertyDocument)returnedDoc));

            PartitionKey key = new PartitionKey(args.ValueToPartitionKey(args.Value));
            response = await container.Items.ReadItemAsync<SpecialPropertyDocument>(key, response.Resource.id);
            returnedDoc = response.Resource;
            Assert.AreEqual(args.Value, getPropertyValueFunction((SpecialPropertyDocument)returnedDoc));

            returnedDoc = (await this.RunSinglePartitionQuery<SpecialPropertyDocument>(
                container,
                "SELECT * FROM t",
                key)).Single();

            Assert.AreEqual(args.Value, getPropertyValueFunction(returnedDoc));

            string query;
            switch (args.Name)
            {
                case "Guid":
                    query = $"SELECT * FROM T WHERE T.Guid = '{(Guid)args.Value}'";
                    break;
                case "Enum":
                    query = $"SELECT * FROM T WHERE T.Enum = '{(HttpStatusCode)args.Value}'";
                    break;
                case "DateTime":
                    query = $"SELECT * FROM T WHERE T.DateTime = '{(DateTime)args.Value}'";
                    break;
                case "CustomEnum":
                    query = $"SELECT * FROM T WHERE T.CustomEnum = '{(HttpStatusCode)args.Value}'";
                    break;
                case "ResourceId":
                    query = $"SELECT * FROM T WHERE T.ResourceId = '{(string)args.Value}'";
                    break;
                case "CustomDateTime":
                    query = $"SELECT * FROM T WHERE T.CustomDateTime = '{(DateTime)args.Value}'";
                    break;
                default:
                    query = null;
                    break;
            }

            returnedDoc = (await container.Items.CreateItemQuery<SpecialPropertyDocument>(
                query,
                partitionKey: args.ValueToPartitionKey,
                maxItemCount: 1).FetchNextSetAsync()).First();

            Assert.AreEqual(args.Value, getPropertyValueFunction(returnedDoc));
        }

        private sealed class SpecialPropertyDocument
        {
            public string id
            {
                get;
                set;
            }

            public Guid Guid
            {
                get;
                set;
            }

            [JsonConverter(typeof(IsoDateTimeConverter))]
            public DateTime DateTime
            {
                get;
                set;
            }

            [JsonConverter(typeof(EpochDateTimeConverter))]
            public DateTime CustomDateTime
            {
                get;
                set;
            }


            public HttpStatusCode Enum
            {
                get;
                set;
            }

            [JsonConverter(typeof(StringEnumConverter))]
            public HttpStatusCode CustomEnum
            {
                get;
                set;
            }

            public string ResourceId
            {
                get;
                set;
            }
        }

        private sealed class EpochDateTimeConverter : JsonConverter
        {
            public static int DateTimeToEpoch(DateTime dt)
            {
                if (!dt.Equals(DateTime.MinValue))
                {
                    DateTime epoch = new DateTime(1970, 1, 1);
                    TimeSpan epochTimeSpan = dt - epoch;
                    return (int)epochTimeSpan.TotalSeconds;
                }
                else
                {
                    return int.MinValue;
                }
            }

            public override bool CanConvert(Type objectType)
            {
                return true;
            }

            public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
            {
                if (reader.TokenType == JsonToken.None || reader.TokenType == JsonToken.Null)
                {
                    return null;
                }


                if (reader.TokenType != JsonToken.Integer)
                {
                    throw new Exception(
                        string.Format(
                        CultureInfo.InvariantCulture,
                        "Unexpected token parsing date. Expected Integer, got {0}.",
                        reader.TokenType));
                }

                int seconds = Convert.ToInt32(reader.Value, CultureInfo.InvariantCulture);
                return new DateTime(1970, 1, 1).AddSeconds(seconds);
            }

            public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
            {
                int seconds;
                if (value is DateTime)
                {
                    seconds = DateTimeToEpoch((DateTime)value);
                }
                else
                {
                    throw new Exception("Expected date object value.");
                }

                writer.WriteValue(seconds);
            }
        }

        private struct QueryCrossPartitionWithLargeNumberOfKeysArgs
        {
            public int NumberOfDocuments;
            public string PartitionKey;
            public HashSet<int> ExpectedPartitionKeyValues;
        }

        [TestMethod]
        public async Task TestQueryCrossPartitionWithLargeNumberOfKeys()
        {
            int numberOfDocuments = 1000;
            string partitionKey = "key";
            HashSet<int> expectedPartitionKeyValues = new HashSet<int>();
            List<string> documents = new List<string>();
            for (int i = 0; i < numberOfDocuments; i++)
            {
                Document doc = new Document();
                doc.SetPropertyValue(partitionKey, i);
                documents.Add(doc.ToString());

                expectedPartitionKeyValues.Add(i);
            }

            Assert.AreEqual(numberOfDocuments, expectedPartitionKeyValues.Count);

            QueryCrossPartitionWithLargeNumberOfKeysArgs args = new QueryCrossPartitionWithLargeNumberOfKeysArgs()
            {
                NumberOfDocuments = numberOfDocuments,
                PartitionKey = partitionKey,
                ExpectedPartitionKeyValues = expectedPartitionKeyValues,
            };

            await this.CreateIngestQueryDelete<QueryCrossPartitionWithLargeNumberOfKeysArgs>(
                ConnectionModes.Direct | ConnectionModes.Gateway,
                documents,
                this.TestQueryCrossPartitionWithLargeNumberOfKeysHelper,
                args,
                "/" + partitionKey);
        }

        private async Task TestQueryCrossPartitionWithLargeNumberOfKeysHelper(CosmosContainer container, IEnumerable<Document> documents, QueryCrossPartitionWithLargeNumberOfKeysArgs args)
        {
            CosmosSqlQueryDefinition query = new CosmosSqlQueryDefinition(
                $"SELECT VALUE r.{args.PartitionKey} FROM r WHERE ARRAY_CONTAINS(@keys, r.{args.PartitionKey})").UseParameter("@keys", args.ExpectedPartitionKeyValues);

            HashSet<int> actualPartitionKeyValues = new HashSet<int>();
            CosmosResultSetIterator<int> documentQuery = container.Items.CreateItemQuery<int>(
                    sqlQueryDefinition: query,
                    maxItemCount: -1,
                    maxConcurrency: 100);

            while (documentQuery.HasMoreResults)
            {
                CosmosQueryResponse<int> response = await documentQuery.FetchNextSetAsync();
                foreach (int item in response)
                {
                    actualPartitionKeyValues.Add(item);
                }
            }

            Assert.IsTrue(actualPartitionKeyValues.SetEquals(args.ExpectedPartitionKeyValues));
        }

        [TestMethod]
        public async Task TestBasicCrossPartitionQuery()
        {
            int seed = (int)(DateTime.UtcNow - new DateTime(1970, 1, 1)).TotalSeconds;
            uint numberOfDocuments = 100;
            QueryOracle.QueryOracleUtil util = new QueryOracle.QueryOracle2(seed);
            IEnumerable<string> documents = util.GetDocuments(numberOfDocuments);

            await this.CreateIngestQueryDelete(
                ConnectionModes.Direct,
                documents,
                this.TestBasicCrossPartitionQueryHelper);
        }

        private async Task TestBasicCrossPartitionQueryHelper(
            CosmosContainer container,
            IEnumerable<Document> documents)
        {
            foreach (int maxDegreeOfParallelism in new int[] { 1, 100 })
            {
                foreach (int maxItemCount in new int[] { 10, 100 })
                {
                    CosmosQueryRequestOptions feedOptions = new CosmosQueryRequestOptions
                    {
                        EnableCrossPartitionQuery = true,
                        MaxBufferedItemCount = 7000,
                        MaxConcurrency = maxDegreeOfParallelism
                    };

                    List<JToken> actualFromQueryWithoutContinutionTokens;
                    actualFromQueryWithoutContinutionTokens = await QueryWithoutContinuationTokens<JToken>(
                        container,
                        "SELECT * FROM c",
                        maxItemCount,
                        feedOptions);

                    Assert.AreEqual(documents.Count(), actualFromQueryWithoutContinutionTokens.Count);
                }
            }
        }

        [TestMethod]
        public async Task TestQueryCrossPartitionAggregateFunctions()
        {
            AggregateTestArgs aggregateTestArgs = new AggregateTestArgs()
            {
                NumberOfDocsWithSamePartitionKey = 100,
                NumberOfDocumentsDifferentPartitionKey = 100,
                PartitionKey = "key",
                UniquePartitionKey = "uniquePartitionKey",
                Field = "field",
                Values = new object[] { false, true, "abc", "cdfg", "opqrs", "ttttttt", "xyz" },
            };

            List<string> documents = new List<string>(aggregateTestArgs.NumberOfDocumentsDifferentPartitionKey + aggregateTestArgs.NumberOfDocsWithSamePartitionKey);
            foreach (object val in aggregateTestArgs.Values)
            {
                Document doc;
                doc = new Document();
                doc.SetPropertyValue(aggregateTestArgs.PartitionKey, val);
                doc.SetPropertyValue("id", Guid.NewGuid().ToString());

                documents.Add(doc.ToString());
            }

            for (int i = 0; i < aggregateTestArgs.NumberOfDocsWithSamePartitionKey; ++i)
            {
                Document doc = new Document();
                doc.SetPropertyValue(aggregateTestArgs.PartitionKey, aggregateTestArgs.UniquePartitionKey);
                doc.ResourceId = i.ToString(CultureInfo.InvariantCulture);
                doc.SetPropertyValue(aggregateTestArgs.Field, i + 1);
                doc.SetPropertyValue("id", Guid.NewGuid().ToString());

                documents.Add(doc.ToString());
            }

            for (int i = 0; i < aggregateTestArgs.NumberOfDocumentsDifferentPartitionKey; ++i)
            {
                Document doc = new Document();
                doc.SetPropertyValue(aggregateTestArgs.PartitionKey, i + 1);
                doc.SetPropertyValue("id", Guid.NewGuid().ToString());
                documents.Add(doc.ToString());
            }

            await this.CreateIngestQueryDelete<AggregateTestArgs>(
                ConnectionModes.Direct | ConnectionModes.Gateway,
                documents,
                this.TestQueryCrossPartitionAggregateFunctionsAsync,
                aggregateTestArgs,
                "/" + aggregateTestArgs.PartitionKey);
        }

        private struct AggregateTestArgs
        {
            public int NumberOfDocumentsDifferentPartitionKey;
            public int NumberOfDocsWithSamePartitionKey;
            public string PartitionKey;
            public string UniquePartitionKey;
            public string Field;
            public object[] Values;
        }

        private struct AggregateQueryArguments
        {
            public string AggregateOperator;
            public object ExpectedValue;
            public string Predicate;
        }

        private async Task TestQueryCrossPartitionAggregateFunctionsAsync(CosmosContainer container, IEnumerable<Document> documents, AggregateTestArgs aggregateTestArgs)
        {
            int numberOfDocumentsDifferentPartitionKey = aggregateTestArgs.NumberOfDocumentsDifferentPartitionKey;
            int numberOfDocumentSamePartitionKey = aggregateTestArgs.NumberOfDocsWithSamePartitionKey;
            int numberOfDocuments = aggregateTestArgs.NumberOfDocumentsDifferentPartitionKey + aggregateTestArgs.NumberOfDocsWithSamePartitionKey;
            object[] values = aggregateTestArgs.Values;
            string partitionKey = aggregateTestArgs.PartitionKey;

            double samePartitionSum = ((numberOfDocumentSamePartitionKey * (numberOfDocumentSamePartitionKey + 1)) / 2);
            double differentPartitionSum = ((numberOfDocumentsDifferentPartitionKey * (numberOfDocumentsDifferentPartitionKey + 1)) / 2);
            double partitionSum = samePartitionSum + differentPartitionSum;
            AggregateQueryArguments[] aggregateQueryArgumentsList = new AggregateQueryArguments[]
            {
                new AggregateQueryArguments()
                {
                    AggregateOperator = "AVG",
                    ExpectedValue = partitionSum / numberOfDocuments,
                    Predicate = $"IS_NUMBER(r.{partitionKey})",
                },
                new AggregateQueryArguments()
                {
                    AggregateOperator = "AVG",
                    ExpectedValue = Undefined.Value,
                    Predicate = "true",
                },
                new AggregateQueryArguments()
                {
                    AggregateOperator = "COUNT",
                    ExpectedValue = (long)numberOfDocuments + values.Length,
                    Predicate = "true",
                },
                new AggregateQueryArguments()
                {
                    AggregateOperator = "MAX",
                    ExpectedValue = "xyz",
                    Predicate = "true",
                },
                new AggregateQueryArguments()
                {
                    AggregateOperator = "MIN",
                    ExpectedValue = false,
                    Predicate = "true",
                },
                new AggregateQueryArguments()
                {
                    AggregateOperator = "SUM",
                    ExpectedValue = differentPartitionSum,
                    Predicate = $"IS_NUMBER(r.{partitionKey})",
                },
                new AggregateQueryArguments()
                {
                    AggregateOperator = "SUM",
                    ExpectedValue = Undefined.Value,
                    Predicate = $"true",
                },
            };

            foreach (int maxDoP in new[] { 0, 10 })
            {
                foreach (AggregateQueryArguments argument in aggregateQueryArgumentsList)
                {
                    string[] queryFormats = new[]
                    {
                        "SELECT VALUE {0}(r.{1}) FROM r WHERE {2}",
                        "SELECT VALUE {0}(r.{1}) FROM r WHERE {2} ORDER BY r.{1}"
                    };

                    foreach (string queryFormat in queryFormats)
                    {
                        string query = string.Format(CultureInfo.InvariantCulture, queryFormat, argument.AggregateOperator, partitionKey, argument.Predicate);
                        string message = string.Format(CultureInfo.InvariantCulture, "query: {0}, data: {1}", query, JsonConvert.SerializeObject(argument));
                        List<dynamic> items = new List<dynamic>();

                        CosmosResultSetIterator<dynamic> resultSetIterator = container.Items.CreateItemQuery<dynamic>(
                            query,
                            maxConcurrency: maxDoP);
                        while (resultSetIterator.HasMoreResults)
                        {
                            items.AddRange(await resultSetIterator.FetchNextSetAsync());
                        }

                        if (Undefined.Value.Equals(argument.ExpectedValue))
                        {
                            Assert.AreEqual(0, items.Count, message);
                        }
                        else
                        {
                            object expected = argument.ExpectedValue;
                            object actual = items.Single();

                            if (expected is long)
                            {
                                expected = (double)((long)expected);
                            }

                            if (actual is long)
                            {
                                actual = (double)((long)actual);
                            }

                            Assert.AreEqual(expected, actual, message);
                        }
                    }
                }

                // Single partition queries
                double singlePartitionSum = samePartitionSum;
                Tuple<string, object>[] datum = new[]
                {
                    Tuple.Create<string, object>("AVG", singlePartitionSum / numberOfDocumentSamePartitionKey),
                    Tuple.Create<string, object>("COUNT", (long)numberOfDocumentSamePartitionKey),
                    Tuple.Create<string, object>("MAX", (long)numberOfDocumentSamePartitionKey),
                    Tuple.Create<string, object>("MIN", (long)1),
                    Tuple.Create<string, object>("SUM", (long)singlePartitionSum),
                };

                string field = aggregateTestArgs.Field;
                string uniquePartitionKey = aggregateTestArgs.UniquePartitionKey;
                foreach (Tuple<string, object> data in datum)
                {
                    string query = $"SELECT VALUE {data.Item1}(r.{field}) FROM r WHERE r.{partitionKey} = '{uniquePartitionKey}'";
                    dynamic aggregate = (await QueryWithoutContinuationTokens<dynamic>(container, query, maxItemCount: 1)).Single();
                    object expected = data.Item2;

                    if (aggregate is long)
                    {
                        aggregate = (long)aggregate;
                    }

                    if (expected is long)
                    {
                        expected = (long)expected;
                    }

                    Assert.AreEqual(
                        expected,
                        aggregate,
                        string.Format(CultureInfo.InvariantCulture, "query: {0}, data: {1}", query, JsonConvert.SerializeObject(data)));

                    // V3 doesn't support an equivalent to ToList()
                    // Aggregate queries need to be in the form SELECT VALUE <AGGREGATE>
                    //query = $"SELECT {data.Item1}(r.{field}) FROM r WHERE r.{partitionKey} = '{uniquePartitionKey}'";
                    //try
                    //{
                    //     documentClient.CreateDocumentQuery(
                    //      collection,
                    //      query).ToList().Single();
                    //    Assert.Fail($"Expect exception query: {query}");
                    //}
                    //catch (AggregateException ex)
                    //{
                    //    if (!(ex.InnerException is CosmosException) || ((CosmosException)ex.InnerException).StatusCode != HttpStatusCode.BadRequest)
                    //    {
                    //        throw;
                    //    }
                    //}

                    // Make sure ExecuteNextAsync works for unsupported aggregate projection
                    CosmosQueryResponse<dynamic> page = await container.Items.CreateItemQuery<dynamic>(query, maxConcurrency: 1).FetchNextSetAsync();
                }
            }
        }

        [TestMethod]
        public async Task TestQueryCrossPartitionAggregateFunctionsEmptyPartitions()
        {
            AggregateQueryEmptyPartitionsArgs args = new AggregateQueryEmptyPartitionsArgs()
            {
                NumDocuments = 100,
                PartitionKey = "key",
                UniqueField = "UniqueField",
            };

            List<string> documents = new List<string>(args.NumDocuments);
            for (int i = 0; i < args.NumDocuments; ++i)
            {
                Document doc = new Document();
                doc.SetPropertyValue(args.PartitionKey, Guid.NewGuid());
                doc.SetPropertyValue(args.UniqueField, i);
                documents.Add(doc.ToString());
            }

            await this.CreateIngestQueryDelete<AggregateQueryEmptyPartitionsArgs>(
                ConnectionModes.Direct | ConnectionModes.Gateway,
                documents,
                this.TestQueryCrossPartitionAggregateFunctionsEmptyPartitionsHelper,
                args,
                "/" + args.PartitionKey);
        }

        private struct AggregateQueryEmptyPartitionsArgs
        {
            public int NumDocuments;
            public string PartitionKey;
            public string UniqueField;
        }

        private async Task TestQueryCrossPartitionAggregateFunctionsEmptyPartitionsHelper(CosmosContainer container, IEnumerable<Document> documents, AggregateQueryEmptyPartitionsArgs args)
        {
            await CrossPartitionQueryTests.NoOp();
            int numDocuments = args.NumDocuments;
            string partitionKey = args.PartitionKey;
            string uniqueField = args.UniqueField;

            // Perform full fanouts but only match a single value that isn't the partition key.
            // This leads to all other partitions returning { "<aggregate>" = UNDEFINDED, "count" = 0 }
            // which should be ignored from the aggregation.
            int valueOfInterest = args.NumDocuments / 2;
            string[] queries = new string[]
            {
                $"SELECT VALUE AVG(c.{uniqueField}) FROM c WHERE c.{uniqueField} = {valueOfInterest}",
                $"SELECT VALUE MIN(c.{uniqueField}) FROM c WHERE c.{uniqueField} = {valueOfInterest}",
                $"SELECT VALUE MAX(c.{uniqueField}) FROM c WHERE c.{uniqueField} = {valueOfInterest}",
                $"SELECT VALUE SUM(c.{uniqueField}) FROM c WHERE c.{uniqueField} = {valueOfInterest}",
            };

            foreach (string query in queries)
            {
                try
                {
                    List<dynamic> items = await this.RunQuery<dynamic>(
                    container,
                    query,
                    maxConcurrency: 10);

                    Assert.AreEqual(valueOfInterest, items.Single());
                }
                catch (Exception ex)
                {
                    Assert.Fail($"Something went wrong with query: {query}, ex: {ex}");
                }
            }
        }

        [TestMethod]
        public async Task TestQueryCrossPartitionAggregateFunctionsWithMixedTypes()
        {
            AggregateQueryMixedTypes args = new AggregateQueryMixedTypes()
            {
                PartitionKey = "key",
                Field = "field",
                DoubleOnlyKey = "doubleOnly",
                StringOnlyKey = "stringOnly",
                BoolOnlyKey = "boolOnly",
                NullOnlyKey = "nullOnly",
                ObjectOnlyKey = "objectOnlyKey",
                ArrayOnlyKey = "arrayOnlyKey",
                OneObjectKey = "oneObjectKey",
                OneArrayKey = "oneArrayKey",
                UndefinedKey = "undefinedKey",
            };

            List<string> documents = new List<string>();
            Random random = new Random(1234);
            for (int i = 0; i < 20; ++i)
            {
                Document doubleDoc = new Document();
                doubleDoc.SetPropertyValue(args.PartitionKey, Guid.NewGuid());
                doubleDoc.SetPropertyValue(args.Field, random.Next(1, 100000));
                documents.Add(doubleDoc.ToString());
                doubleDoc.SetPropertyValue(args.PartitionKey, args.DoubleOnlyKey);
                documents.Add(doubleDoc.ToString());

                Document stringDoc = new Document();
                stringDoc.SetPropertyValue(args.PartitionKey, Guid.NewGuid());
                stringDoc.SetPropertyValue(args.Field, random.NextDouble().ToString());
                documents.Add(stringDoc.ToString());
                stringDoc.SetPropertyValue(args.PartitionKey, args.StringOnlyKey);
                documents.Add(stringDoc.ToString());

                Document boolDoc = new Document();
                boolDoc.SetPropertyValue(args.PartitionKey, Guid.NewGuid());
                boolDoc.SetPropertyValue(args.Field, random.Next() % 2 == 0);
                documents.Add(boolDoc.ToString());
                boolDoc.SetPropertyValue(args.PartitionKey, args.BoolOnlyKey);
                documents.Add(boolDoc.ToString());

                Document nullDoc = new Document();
                nullDoc.SetPropertyValue(args.PartitionKey, Guid.NewGuid());
                nullDoc.propertyBag.Add(args.Field, null);
                documents.Add(nullDoc.ToString());
                nullDoc.SetPropertyValue(args.PartitionKey, args.NullOnlyKey);
                documents.Add(nullDoc.ToString());

                Document objectDoc = new Document();
                objectDoc.SetPropertyValue(args.PartitionKey, Guid.NewGuid());
                objectDoc.SetPropertyValue(args.Field, new object { });
                documents.Add(objectDoc.ToString());
                objectDoc.SetPropertyValue(args.PartitionKey, args.ObjectOnlyKey);
                documents.Add(objectDoc.ToString());

                Document arrayDoc = new Document();
                arrayDoc.SetPropertyValue(args.PartitionKey, Guid.NewGuid());
                arrayDoc.SetPropertyValue(args.Field, new object[] { });
                documents.Add(arrayDoc.ToString());
                arrayDoc.SetPropertyValue(args.PartitionKey, args.ArrayOnlyKey);
                documents.Add(arrayDoc.ToString());
            }

            Document oneObjectDoc = new Document();
            oneObjectDoc.SetPropertyValue(args.PartitionKey, args.OneObjectKey);
            oneObjectDoc.SetPropertyValue(args.Field, new object { });
            documents.Add(oneObjectDoc.ToString());

            Document oneArrayDoc = new Document();
            oneArrayDoc.SetPropertyValue(args.PartitionKey, args.OneArrayKey);
            oneArrayDoc.SetPropertyValue(args.Field, new object[] { });
            documents.Add(oneArrayDoc.ToString());

            Document undefinedDoc = new Document();
            undefinedDoc.SetPropertyValue(args.PartitionKey, args.UndefinedKey);
            // This doc does not have the field key set
            documents.Add(undefinedDoc.ToString());

            await this.CreateIngestQueryDelete<AggregateQueryMixedTypes>(
                ConnectionModes.Direct,
                documents,
                this.TestQueryCrossPartitionAggregateFunctionsWithMixedTypesHelper,
                args,
                "/" + args.PartitionKey);
        }

        private struct AggregateQueryMixedTypes
        {
            public string PartitionKey;
            public string Field;
            public string DoubleOnlyKey;
            public string StringOnlyKey;
            public string BoolOnlyKey;
            public string NullOnlyKey;
            public string ObjectOnlyKey;
            public string ArrayOnlyKey;
            public string OneObjectKey;
            public string OneArrayKey;
            public string UndefinedKey;
        }

        private async Task TestQueryCrossPartitionAggregateFunctionsWithMixedTypesHelper(
            CosmosContainer container,
            IEnumerable<Document> documents,
            AggregateQueryMixedTypes args)
        {
            await CrossPartitionQueryTests.NoOp();
            string partitionKey = args.PartitionKey;
            string field = args.Field;
            string[] typeOnlyPartitionKeys = new string[]
            {
                args.DoubleOnlyKey,
                args.StringOnlyKey,
                args.BoolOnlyKey,
                args.NullOnlyKey,
                args.ObjectOnlyKey,
                args.ArrayOnlyKey,
                args.OneArrayKey,
                args.OneObjectKey,
                args.UndefinedKey
            };

            string[] aggregateOperators = new string[] { "AVG", "MIN", "MAX", "SUM", "COUNT" };
            string[] typeCheckFunctions = new string[] { "IS_ARRAY", "IS_BOOL", "IS_NULL", "IS_NUMBER", "IS_OBJECT", "IS_STRING", "IS_DEFINED", "IS_PRIMITIVE" };
            List<string> queries = new List<string>();
            foreach (string aggregateOperator in aggregateOperators)
            {
                foreach (string typeCheckFunction in typeCheckFunctions)
                {
                    queries.Add(
                    $@"
                        SELECT VALUE {aggregateOperator} (c.{field}) 
                        FROM c 
                        WHERE {typeCheckFunction}(c.{field})
                    ");
                }

                foreach (string typeOnlyPartitionKey in typeOnlyPartitionKeys)
                {
                    queries.Add(
                    $@"
                        SELECT VALUE {aggregateOperator} (c.{field}) 
                        FROM c 
                        WHERE c.{partitionKey} = ""{typeOnlyPartitionKey}""
                    ");
                }
            };

            // mixing primitive and non primitives
            foreach (string minmaxop in new string[] { "MIN", "MAX" })
            {
                foreach (string key in new string[] { args.OneObjectKey, args.OneArrayKey })
                {
                    queries.Add(
                    $@"
                        SELECT VALUE {minmaxop} (c.{field}) 
                        FROM c 
                        WHERE c.{partitionKey} IN (""{key}"", ""{args.DoubleOnlyKey}"")
                    ");
                }
            }


            string filename = $"CrossPartitionQueryTests.AggregateMixedTypes";
            string outputPath = $"{filename}_output.xml";
            string baselinePath = $"{filename}_baseline.xml";
            XmlWriterSettings settings = new XmlWriterSettings()
            {
                OmitXmlDeclaration = true,
                Indent = true,
                NewLineOnAttributes = true,
            };
            using (XmlWriter writer = XmlWriter.Create(outputPath, settings))
            {
                writer.WriteStartDocument();
                writer.WriteStartElement("Results");
                foreach (string query in queries)
                {
                    string formattedQuery = string.Join(
                        Environment.NewLine,
                        query.Trim().Split(
                            new[] { Environment.NewLine },
                            StringSplitOptions.None)
                            .Select(x => x.Trim()));

                    List<dynamic> items = await this.RunQuery<dynamic>(
                        container,
                        query,
                        10,
                        null);

                    writer.WriteStartElement("Result");
                    writer.WriteStartElement("Query");
                    writer.WriteCData(formattedQuery);
                    writer.WriteEndElement();
                    writer.WriteStartElement("Aggregation");
                    if (items.Count > 0)
                    {
                        writer.WriteCData(JsonConvert.SerializeObject(items.Single()));
                    }
                    writer.WriteEndElement();
                    writer.WriteEndElement();
                }
                writer.WriteEndElement();
                writer.WriteEndDocument();
            }

            Assert.AreEqual(File.ReadAllText(baselinePath), File.ReadAllText(outputPath));
        }

        [TestMethod]
        public async Task TestQueryDistinct()
        {
            int seed = (int)(DateTime.UtcNow - new DateTime(1970, 1, 1)).TotalSeconds;
            uint numberOfDocuments = 100;

            Random rand = new Random(seed);
            List<Person> people = new List<Person>();

            for (int i = 0; i < numberOfDocuments; i++)
            {
                Person person = CrossPartitionQueryTests.GetRandomPerson(rand);
                for (int j = 0; j < rand.Next(0, 4); j++)
                {
                    people.Add(person);
                }
            }

            List<string> documents = new List<string>();
            people = people.OrderBy((person) => Guid.NewGuid()).ToList();
            foreach (Person person in people)
            {
                documents.Add(JsonConvert.SerializeObject(person));
            }

            await this.CreateIngestQueryDelete(
                ConnectionModes.Direct | ConnectionModes.Gateway,
                documents,
                this.TestQueryDistinct,
                "/id");
        }

        private async Task TestQueryDistinct(CosmosContainer container, IEnumerable<Document> documents, dynamic testArgs = null)
        {
            #region Queries
            // To verify distint queries you can run it once without the distinct clause and run it through a hash set 
            // then compare to the query with the distinct clause.
            List<string> queries = new List<string>()
            {
                // basic distinct queries
                "SELECT {0} VALUE null",
                "SELECT {0} VALUE false",
                "SELECT {0} VALUE true",
                "SELECT {0} VALUE 1",
                "SELECT {0} VALUE 'a'",
                "SELECT {0} VALUE [null, true, false, 1, 'a']",
                "SELECT {0} VALUE {{p1:null, p2:true, p3:false, p4:1, p5:'a'}}",
                "SELECT {0} false AS p",
                "SELECT {0} 1 AS p",
                "SELECT {0} 'a' AS p",
                "SELECT {0} [null, true, false, 1, 'a'] AS p",
                "SELECT {0} {{p1:null, p2:true, p3:false, p4:1, p5:'a'}} AS p",
                "SELECT {0} VALUE {{p1:null, p2:true, p3:false, p4:1, p5:'a'}}",
                "SELECT {0} VALUE null FROM c",
                "SELECT {0} VALUE false FROM c",
                "SELECT {0} VALUE 1 FROM c",
                "SELECT {0} VALUE 'a' FROM c",
                "SELECT {0} VALUE [null, true, false, 1, 'a'] FROM c",
                "SELECT {0} null AS p FROM c",
                "SELECT {0} false AS p FROM c",
                "SELECT {0} 1 AS p FROM c",
                "SELECT {0} 'a' AS p FROM c",
                "SELECT {0} [null, true, false, 1, 'a'] AS p FROM c",
                "SELECT {0} {{p1:null, p2:true, p3:false, p4:1, p5:'a'}} AS p FROM c",

                // number value distinct queries
                "SELECT {0} VALUE c.income from c",
                "SELECT {0} VALUE c.age from c",
                "SELECT {0} c.income, c.income AS income2 from c",
                "SELECT {0} c.income, c.age from c",
                "SELECT {0} VALUE [c.income, c.age] from c",

                // string value distinct queries
                "SELECT {0} VALUE c.name from c",
                "SELECT {0} VALUE c.city from c",
                "SELECT {0} VALUE c.partitionKey from c",
                "SELECT {0} c.name, c.name AS name2 from c",
                "SELECT {0} c.name, c.city from c",
                "SELECT {0} VALUE [c.name, c.city] from c",

                // array value distinct queries
                "SELECT {0} VALUE c.children from c",
                "SELECT {0} c.children, c.children AS children2 from c",
                "SELECT {0} VALUE [c.name, c.age, c.pet] from c",

                // object value distinct queries
                "SELECT {0} VALUE c.pet from c",
                "SELECT {0} c.pet, c.pet AS pet2 from c",

                // scalar expressions distinct query
                "SELECT {0} VALUE c.age % 2 FROM c",
                "SELECT {0} VALUE ABS(c.age) FROM c",
                "SELECT {0} VALUE LEFT(c.name, 1) FROM c",
                "SELECT {0} VALUE c.name || ', ' || (c.city ?? '') FROM c",
                "SELECT {0} VALUE ARRAY_LENGTH(c.children) FROM c",
                "SELECT {0} VALUE IS_DEFINED(c.city) FROM c",
                "SELECT {0} VALUE (c.children[0].age ?? 0) + (c.children[1].age ?? 0) FROM c",

                // distinct queries with order by
                "SELECT {0} VALUE c.age FROM c ORDER BY c.age",
                "SELECT {0} VALUE c.name FROM c ORDER BY c.name",
                "SELECT {0} VALUE c.city FROM c ORDER BY c.city",
                "SELECT {0} VALUE c.city FROM c ORDER BY c.age",
                "SELECT {0} VALUE LEFT(c.name, 1) FROM c ORDER BY c.name",

                // distinct queries with top and no matching order by
                "SELECT {0} TOP 2147483647 VALUE c.age FROM c",

                // distinct queries with top and  matching order by
                "SELECT {0} TOP 2147483647 VALUE c.age FROM c ORDER BY c.age",

                // distinct queries with aggregates
                "SELECT {0} VALUE MAX(c.age) FROM c",

                // distinct queries with joins
                "SELECT {0} VALUE c.age FROM p JOIN c IN p.children",
                "SELECT {0} p.age AS ParentAge, c.age ChildAge FROM p JOIN c IN p.children",
                "SELECT {0} VALUE c.name FROM p JOIN c IN p.children",
                "SELECT {0} p.name AS ParentName, c.name ChildName FROM p JOIN c IN p.children",

                // distinct queries in subqueries
                "SELECT {0} r.age, s FROM r JOIN (SELECT DISTINCT VALUE c FROM (SELECT 1 a) c) s WHERE r.age > 25",
                "SELECT {0} p.name, p.age FROM (SELECT DISTINCT * FROM r) p WHERE p.age > 25",

                // distinct queries in scalar subqeries
                "SELECT {0} p.name, (SELECT DISTINCT VALUE p.age) AS Age FROM p",
                "SELECT {0} p.name, p.age FROM p WHERE (SELECT DISTINCT VALUE LEFT(p.name, 1)) > 'A' AND (SELECT DISTINCT VALUE p.age) > 21",
                "SELECT {0} p.name, (SELECT DISTINCT VALUE p.age) AS Age FROM p WHERE (SELECT DISTINCT VALUE p.name) > 'A' OR (SELECT DISTINCT VALUE p.age) > 21",

                // select *
                "SELECT {0} * FROM c",
            };
            #endregion
            #region ExecuteNextAsync API
            // run the query with distinct and without + MockDistinctMap
            // Should receive same results
            // PageSize = 1 guarantees that the backend will return some duplicates.
            foreach (string query in queries)
            {
                foreach (int pageSize in new int[] { 1, 10, 100 })
                {
                    string queryWithDistinct = string.Format(query, "DISTINCT");
                    string queryWithoutDistinct = string.Format(query, "");
                    MockDistinctMap documentsSeen = new MockDistinctMap();
                    List<JToken> documentsFromWithDistinct = new List<JToken>();
                    List<JToken> documentsFromWithoutDistinct = new List<JToken>();

                    CosmosResultSetIterator<JToken> documentQueryWithoutDistinct = container.Items.CreateItemQuery<JToken>(
                        queryWithoutDistinct,
                        maxConcurrency: 100,
                        maxItemCount: pageSize);

                    while (documentQueryWithoutDistinct.HasMoreResults)
                    {
                        CosmosQueryResponse<JToken> cosmosQueryResponse = await documentQueryWithoutDistinct.FetchNextSetAsync();
                        foreach (JToken document in cosmosQueryResponse)
                        {
                            if (documentsSeen.Add(document, out UInt192? hash))
                            {
                                documentsFromWithoutDistinct.Add(document);
                            }
                            else
                            {
                                // No Op for debugging purposes.
                            }
                        }
                    }

                    CosmosResultSetIterator<JToken> documentQueryWithDistinct = container.Items.CreateItemQuery<JToken>(
                        queryWithDistinct,
                        maxConcurrency: 100,
                        maxItemCount: pageSize);

                    while (documentQueryWithDistinct.HasMoreResults)
                    {
                        CosmosQueryResponse<JToken> cosmosQueryResponse = await documentQueryWithDistinct.FetchNextSetAsync();
                        documentsFromWithDistinct.AddRange(cosmosQueryResponse);
                    }

                    try
                    {
                        Assert.AreEqual(documentsFromWithDistinct.Count, documentsFromWithoutDistinct.Count());
                        for (int i = 0; i < documentsFromWithDistinct.Count; i++)
                        {
                            JToken documentFromWithDistinct = documentsFromWithDistinct.ElementAt(i);
                            JToken documentFromWithoutDistinct = documentsFromWithoutDistinct.ElementAt(i);
                            Assert.IsTrue(
                                JsonTokenEqualityComparer.Value.Equals(documentFromWithDistinct, documentFromWithoutDistinct),
                                $"{documentFromWithDistinct} did not match {documentFromWithoutDistinct} at index {i} for {queryWithDistinct}, with page size: {pageSize} on a container");
                        }
                    }
                    catch (Exception e)
                    {
                        throw e;
                    }
                }
            }
            #endregion
            #region Unordered Continuation
            // Run the unordered distinct query through the continuation api should result in the same set(but maybe some duplicates)
            foreach (string query in new string[]
            {
                "SELECT {0} VALUE c.name from c",
                "SELECT {0} VALUE c.age from c",
                "SELECT {0} TOP 2147483647 VALUE c.city from c",
                "SELECT {0} VALUE c.age from c ORDER BY c.name",
            })
            {
                string queryWithDistinct = string.Format(query, "DISTINCT");
                string queryWithoutDistinct = string.Format(query, "");
                HashSet<JToken> documentsFromWithDistinct = new HashSet<JToken>(JsonTokenEqualityComparer.Value);
                HashSet<JToken> documentsFromWithoutDistinct = new HashSet<JToken>(JsonTokenEqualityComparer.Value);

                CosmosResultSetIterator<JToken> documentQueryWithoutDistinct = container.Items.CreateItemQuery<JToken>(
                        queryWithoutDistinct,
                        maxItemCount: 10,
                        maxConcurrency: 100);

                while (documentQueryWithoutDistinct.HasMoreResults)
                {
                    CosmosQueryResponse<JToken> cosmosQueryResponse = await documentQueryWithoutDistinct.FetchNextSetAsync();
                    foreach (JToken jToken in cosmosQueryResponse)
                    {
                        documentsFromWithoutDistinct.Add(jToken);
                    }
                }

                CosmosResultSetIterator<JToken> documentQueryWithDistinct = container.Items.CreateItemQuery<JToken>(
                    queryWithDistinct,
                    maxItemCount: 10,
                    maxConcurrency: 100);

                // For now we are blocking the use of continuation 
                // This try catch can be removed if we do allow the continuation token.
                try
                {
                    string continuationToken = null;
                    do
                    {
                        CosmosResultSetIterator<JToken> documentQuery = container.Items.CreateItemQuery<JToken>(
                            queryWithDistinct,
                            maxItemCount: 10,
                            maxConcurrency: 100);

                        CosmosQueryResponse<JToken> cosmosQueryResponse = await documentQuery.FetchNextSetAsync();
                        foreach (JToken jToken in cosmosQueryResponse)
                        {
                            documentsFromWithDistinct.Add(jToken);
                        }

                        continuationToken = cosmosQueryResponse.ContinuationToken;

                    }
                    while (continuationToken != null);
                    Assert.IsTrue(
                        documentsFromWithDistinct.IsSubsetOf(documentsFromWithoutDistinct),
                        $"Documents didn't match for {queryWithDistinct} on a Partitioned container");

                    Assert.Fail("Expected an exception when using continuation tokens on an unordered distinct query.");
                }
                catch (ArgumentException ex)
                {
                    string disallowContinuationErrorMessage = RMResources.UnorderedDistinctQueryContinuationToken;
                    Assert.AreEqual(disallowContinuationErrorMessage, ex.Message);
                }
            }
            #endregion
            #region Ordered Region
            // Run the ordered distinct query through the continuation api, should result in the same set
            // since the previous hash is passed in the continuation token.
            foreach (string query in new string[]
            {
                "SELECT {0} VALUE c.age FROM c ORDER BY c.age",
                "SELECT {0} VALUE c.name FROM c ORDER BY c.name",
            })
            {
                foreach (int pageSize in new int[] { 1, 10, 100 })
                {
                    string queryWithDistinct = string.Format(query, "DISTINCT");
                    string queryWithoutDistinct = string.Format(query, "");
                    MockDistinctMap documentsSeen = new MockDistinctMap();
                    List<JToken> documentsFromWithDistinct = new List<JToken>();
                    List<JToken> documentsFromWithoutDistinct = new List<JToken>();

                    CosmosResultSetIterator<JToken> documentQueryWithoutDistinct = container.Items.CreateItemQuery<JToken>(
                        sqlQueryText: queryWithoutDistinct,
                        maxConcurrency: 100,
                        maxItemCount: 1);

                    while (documentQueryWithoutDistinct.HasMoreResults)
                    {
                        CosmosQueryResponse<JToken> cosmosQueryResponse = await documentQueryWithoutDistinct.FetchNextSetAsync();
                        foreach (JToken document in cosmosQueryResponse)
                        {
                            if (documentsSeen.Add(document, out UInt192? hash))
                            {
                                documentsFromWithoutDistinct.Add(document);
                            }
                            else
                            {
                                // No Op for debugging purposes.
                            }
                        }
                    }

                    CosmosResultSetIterator<JToken> documentQueryWithDistinct = container.Items.CreateItemQuery<JToken>(
                       sqlQueryText: queryWithDistinct,
                       maxConcurrency: 100,
                       maxItemCount: 1);

                    string continuationToken = null;
                    do
                    {
                        CosmosResultSetIterator<JToken> cosmosQuery = container.Items.CreateItemQuery<JToken>(
                                   sqlQueryText: queryWithDistinct,
                                   maxConcurrency: 100,
                                   maxItemCount: 1,
                                   continuationToken: continuationToken);

                        CosmosQueryResponse<JToken> cosmosQueryResponse = await cosmosQuery.FetchNextSetAsync();
                        documentsFromWithDistinct.AddRange(cosmosQueryResponse);
                        continuationToken = cosmosQueryResponse.ContinuationToken;
                    }
                    while (continuationToken != null);

                    Assert.IsTrue(
                        documentsFromWithDistinct.SequenceEqual(documentsFromWithoutDistinct, JsonTokenEqualityComparer.Value),
                        $"Documents didn't match for {queryWithDistinct} on a Partitioned container");
                }
            }
            #endregion
        }

        [TestMethod]
        public async Task TestQueryCrossPartitionTopOrderByDifferentDimension()
        {
            string[] documents = new[]
            {
                @"{""id"":""documentId1"",""key"":""A""}",
                @"{""id"":""documentId2"",""key"":""A"",""prop"":3}",
                @"{""id"":""documentId3"",""key"":""A""}",
                @"{""id"":""documentId4"",""key"":5}",
                @"{""id"":""documentId5"",""key"":5,""prop"":2}",
                @"{""id"":""documentId6"",""key"":5}",
                @"{""id"":""documentId7"",""key"":2}",
                @"{""id"":""documentId8"",""key"":2,""prop"":1}",
                @"{""id"":""documentId9"",""key"":2}",
            };

            await this.CreateIngestQueryDelete(
                ConnectionModes.Direct | ConnectionModes.Gateway,
                documents,
                this.TestQueryCrossPartitionTopOrderByDifferentDimensionHelper,
                "/key");
        }

        private async Task TestQueryCrossPartitionTopOrderByDifferentDimensionHelper(CosmosContainer container, IEnumerable<Document> documents)
        {
            await CrossPartitionQueryTests.NoOp();

            string[] expected = new[] { "documentId2", "documentId5", "documentId8" };
            List<Document> query = await this.RunQuery<Document>(
                container,
                "SELECT r.id FROM r ORDER BY r.prop DESC",
                maxItemCount: 1,
                maxConcurrency: 1);

            Assert.AreEqual(string.Join(", ", expected), string.Join(", ", query.Select(doc => doc.Id)));
        }

        [TestMethod]
        [Ignore] // TODO  https://msdata.visualstudio.com/CosmosDB/_workitems/edit/408486
        public async Task TestMixedTypeOrderBy()
        {
            int numberOfDocuments = 1 << 4;
            int numberOfDuplicates = 1 << 2;

            List<string> documents = new List<string>(numberOfDocuments * numberOfDuplicates);
            Random random = new Random(1234);
            for (int i = 0; i < numberOfDocuments; ++i)
            {
                MixedTypedDocument mixedTypeDocument = CrossPartitionQueryTests.GenerateMixedTypeDocument(random);
                for (int j = 0; j < numberOfDuplicates; j++)
                {
                    documents.Add(JsonConvert.SerializeObject(mixedTypeDocument)); ;
                }
            }

            // Just have range indexes
            Cosmos.IndexingPolicy indexV1Policy = new Cosmos.IndexingPolicy()
            {
                IncludedPaths = new Collection<Cosmos.IncludedPath>()
                {
                    new Cosmos.IncludedPath()
                    {
                        Path = "/*",
                        Indexes = new Collection<Cosmos.Index>()
                        {
                            Cosmos.Index.Range(Cosmos.DataType.String, -1),
                            Cosmos.Index.Range(Cosmos.DataType.Number, -1),
                        }
                    }
                }
            };

            // Add a composite index to force an index v2 container to be made.
            Cosmos.IndexingPolicy indexV2Policy = new Cosmos.IndexingPolicy()
            {
                IncludedPaths = new Collection<Cosmos.IncludedPath>()
                {
                    new Cosmos.IncludedPath()
                    {
                        Path = "/*",
                    }
                },

                CompositeIndexes = new Collection<Collection<Cosmos.CompositePath>>()
                {
                    // Simple
                    new Collection<Cosmos.CompositePath>()
                    {
                        new Cosmos.CompositePath()
                        {
                            Path = "/_ts",
                        },
                        new Cosmos.CompositePath()
                        {
                            Path = "/_etag",
                        }
                    }
                }
            };

            string indexV2Api = HttpConstants.Versions.v2018_09_17;
            string indexV1Api = HttpConstants.Versions.v2017_11_15;

            Func<bool, OrderByTypes[], Action<Exception>, Task> runWithAllowMixedTypeOrderByFlag = async (allowMixedTypeOrderByTestFlag, orderByTypes, expectedExcpetionHandler) =>
            {
                bool allowMixedTypeOrderByTestFlagOriginalValue = OrderByConsumeComparer.AllowMixedTypeOrderByTestFlag;
                string apiVersion = allowMixedTypeOrderByTestFlag ? indexV2Api : indexV1Api;
                Cosmos.IndexingPolicy indexingPolicy = allowMixedTypeOrderByTestFlag ? indexV2Policy : indexV1Policy;
                try
                {
                    OrderByConsumeComparer.AllowMixedTypeOrderByTestFlag = allowMixedTypeOrderByTestFlag;
                    await this.RunWithApiVersion(
                        apiVersion,
                        async () =>
                        {
                            await this.CreateIngestQueryDelete<Tuple<OrderByTypes[], Action<Exception>>>(
                                ConnectionModes.Direct,
                                documents,
                                this.TestMixedTypeOrderByHelper,
                                new Tuple<OrderByTypes[], Action<Exception>>(orderByTypes, expectedExcpetionHandler),
                                "/id",
                                indexingPolicy);
                        });
                }
                finally
                {
                    OrderByConsumeComparer.AllowMixedTypeOrderByTestFlag = allowMixedTypeOrderByTestFlagOriginalValue;
                }
            };

            bool dontAllowMixedTypes = false;
            bool doAllowMixedTypes = true;

            OrderByTypes primitives = OrderByTypes.Bool | OrderByTypes.Null | OrderByTypes.Number | OrderByTypes.String;
            OrderByTypes nonPrimitives = OrderByTypes.Array | OrderByTypes.Object;
            OrderByTypes all = primitives | nonPrimitives | OrderByTypes.Undefined;

            // Don't allow mixed types but single type order by should still work
            await runWithAllowMixedTypeOrderByFlag(
                dontAllowMixedTypes,
                new OrderByTypes[]
                {
                    OrderByTypes.Array,
                    OrderByTypes.Bool,
                    OrderByTypes.Null,
                    OrderByTypes.Number,
                    OrderByTypes.Object,
                    OrderByTypes.String,
                    OrderByTypes.Undefined,
                }, null);

            // If you don't allow mixed types but you run a mixed type query then you should get an exception or the results are just wrong.
            await runWithAllowMixedTypeOrderByFlag(
                dontAllowMixedTypes,
                new OrderByTypes[]
                {
                    all,
                    primitives,
                },
                (exception) =>
                {
                    Assert.IsTrue(
                        // Either we get the weird client exception for having mixed types
                        exception.Message.Contains("Cannot execute cross partition order-by queries on mix types.")
                        // Or the results are just messed up since the pages in isolation were not mixed typed.
                        || exception.GetType() == typeof(AssertFailedException));
                });

            // Mixed type orderby should work for all scenarios,
            // since for now the non primitives are accepted to not be served from the index.
            await runWithAllowMixedTypeOrderByFlag(
                doAllowMixedTypes,
                new OrderByTypes[]
                {
                    OrderByTypes.Array,
                    OrderByTypes.Bool,
                    OrderByTypes.Null,
                    OrderByTypes.Number,
                    OrderByTypes.Object,
                    OrderByTypes.String,
                    OrderByTypes.Undefined,
                    primitives,
                    nonPrimitives,
                    all,
                }, null);
        }

        private sealed class MixedTypedDocument
        {
            public object MixedTypeField { get; set; }
        }

        private static MixedTypedDocument GenerateMixedTypeDocument(Random random)
        {
            return new MixedTypedDocument()
            {
                MixedTypeField = GenerateRandomJsonValue(random),
            };
        }

        private static object GenerateRandomJsonValue(Random random)
        {
            switch (random.Next(0, 6))
            {
                // Number
                case 0:
                    return random.Next();
                // String
                case 1:
                    return new string('a', random.Next(0, 100));
                // Null
                case 2:
                    return null;
                // Bool
                case 3:
                    return (random.Next() % 2) == 0;
                // Object
                case 4:
                    return new object();
                // Array
                case 5:
                    return new List<object>();
                default:
                    throw new ArgumentException();
            }
        }

        private sealed class MockOrderByComparer : IComparer<object>
        {
            public static readonly MockOrderByComparer Value = new MockOrderByComparer();

            public int Compare(object x, object y)
            {
                CosmosElement element1 = ObjectToCosmosElement(x);
                CosmosElement element2 = ObjectToCosmosElement(y);

                return ItemComparer.Instance.Compare(element1, element2);
            }

            private static CosmosElement ObjectToCosmosElement(object obj)
            {
                string json = JsonConvert.SerializeObject(obj != null ? JToken.FromObject(obj) : JValue.CreateNull());
                byte[] bytes = Encoding.UTF8.GetBytes(json);
                return CosmosElement.Create(bytes);
            }
        }

        [Flags]
        private enum OrderByTypes
        {
            Number = 1 << 0,
            String = 1 << 1,
            Null = 1 << 2,
            Bool = 1 << 3,
            Object = 1 << 4,
            Array = 1 << 5,
            Undefined = 1 << 6,
        };

        private async Task TestMixedTypeOrderByHelper(
            CosmosContainer container,
            IEnumerable<Document> documents,
            Tuple<OrderByTypes[], Action<Exception>> args)
        {
            OrderByTypes[] orderByTypesList = args.Item1;
            Action<Exception> expectedExceptionHandler = args.Item2;
            try
            {
                foreach (bool isDesc in new bool[] { true, false })
                {
                    foreach (OrderByTypes orderByTypes in orderByTypesList)
                    {
                        string orderString = isDesc ? "DESC" : "ASC";
                        List<string> mixedTypeFilters = new List<string>();
                        if (orderByTypes.HasFlag(OrderByTypes.Array))
                        {
                            mixedTypeFilters.Add($"IS_ARRAY(c.{nameof(MixedTypedDocument.MixedTypeField)})");
                        }

                        if (orderByTypes.HasFlag(OrderByTypes.Bool))
                        {
                            mixedTypeFilters.Add($"IS_BOOL(c.{nameof(MixedTypedDocument.MixedTypeField)})");
                        }

                        if (orderByTypes.HasFlag(OrderByTypes.Null))
                        {
                            mixedTypeFilters.Add($"IS_NULL(c.{nameof(MixedTypedDocument.MixedTypeField)})");
                        }

                        if (orderByTypes.HasFlag(OrderByTypes.Number))
                        {
                            mixedTypeFilters.Add($"IS_NUMBER(c.{nameof(MixedTypedDocument.MixedTypeField)})");
                        }

                        if (orderByTypes.HasFlag(OrderByTypes.Object))
                        {
                            mixedTypeFilters.Add($"IS_OBJECT(c.{nameof(MixedTypedDocument.MixedTypeField)})");
                        }

                        if (orderByTypes.HasFlag(OrderByTypes.String))
                        {
                            mixedTypeFilters.Add($"IS_STRING(c.{nameof(MixedTypedDocument.MixedTypeField)})");
                        }

                        if (orderByTypes.HasFlag(OrderByTypes.Undefined))
                        {
                            mixedTypeFilters.Add($"not IS_DEFINED(c.{nameof(MixedTypedDocument.MixedTypeField)})");
                        }

                        string filter = mixedTypeFilters.Count() == 0 ? "true" : string.Join(" OR ", mixedTypeFilters);

                        string query = $@"
                            SELECT VALUE c.{nameof(MixedTypedDocument.MixedTypeField)} 
                            FROM c
                            WHERE {filter}
                            ORDER BY c.{nameof(MixedTypedDocument.MixedTypeField)} {orderString}";

                        CosmosQueryRequestOptions feedOptions = new CosmosQueryRequestOptions()
                        {
                            MaxBufferedItemCount = 1000,
                        };

                        List<JToken> actualFromQueryWithoutContinutionTokens;
                        actualFromQueryWithoutContinutionTokens = await this.RunQuery<JToken>(
                            container,
                            query,
                            maxItemCount: 16,
                            maxConcurrency: 10,
                            requestOptions: feedOptions);
#if false
                        For now we can not serve the query through continuation tokens correctly.
                        This is because we allow order by on mixed types but not comparisions across types
                        For example suppose the following query:
                            SELECT c.MixedTypeField FROM c ORDER BY c.MixedTypeField
                        returns:
                        [
                            {"MixedTypeField":null},
                            {"MixedTypeField":false},
                            {"MixedTypeField":true},
                            {"MixedTypeField":303093052},
                            {"MixedTypeField":438985130},
                            {"MixedTypeField":"aaaaaaaaaaa"}
                        ]
                        and we left off on 303093052 then at some point the cross partition code resumes the query by running the following:
                            SELECT c.MixedTypeField FROM c WHERE c.MixedTypeField > 303093052 ORDER BY c.MixedTypeField
                        which will only return the following:
                            { "MixedTypeField":438985130}
                        and that is because comparision across types is undefined so "aaaaaaaaaaa" > 303093052 never got emitted
#endif

                        IEnumerable<object> insertedDocs = documents
                            .Select(document => document.GetPropertyValue<object>(nameof(MixedTypedDocument.MixedTypeField)));

                        // Build the expected results using LINQ
                        IEnumerable<object> expected = new List<object>();

                        // Filter based on the mixedOrderByType enum
                        if (orderByTypes.HasFlag(OrderByTypes.Array))
                        {
                            // no arrays should be served from the range index
                        }

                        if (orderByTypes.HasFlag(OrderByTypes.Bool))
                        {
                            expected = expected.Concat(insertedDocs.Where(x => x is bool));
                        }

                        if (orderByTypes.HasFlag(OrderByTypes.Null))
                        {
                            expected = expected.Concat(insertedDocs.Where(x => x == null));
                        }

                        if (orderByTypes.HasFlag(OrderByTypes.Number))
                        {
                            expected = expected.Concat(insertedDocs.Where(x => x is double || x is int || x is long));
                        }

                        if (orderByTypes.HasFlag(OrderByTypes.Object))
                        {
                            // no objects should be served from the range index
                        }

                        if (orderByTypes.HasFlag(OrderByTypes.String))
                        {
                            expected = expected.Concat(insertedDocs.Where(x => x is string));
                        }

                        if (orderByTypes.HasFlag(OrderByTypes.Undefined))
                        {
                            // no undefined should be served from the range index
                        }

                        // Order using the mock order by comparer
                        if (isDesc)
                        {
                            expected = expected.OrderByDescending(x => x, MockOrderByComparer.Value);
                        }
                        else
                        {
                            expected = expected.OrderBy(x => x, MockOrderByComparer.Value);
                        }

                        // bind all the value to JTokens so they can be compared agaisnt the actual.
                        List<JToken> expectedBinded = expected.Select(x => x == null ? JValue.CreateNull() : JToken.FromObject(x)).ToList();

                        Assert.IsTrue(
                            expectedBinded.SequenceEqual(actualFromQueryWithoutContinutionTokens, JsonTokenEqualityComparer.Value),
                            $@" queryWithoutContinuations: {query},
                            expected:{JsonConvert.SerializeObject(expected)},
                            actual: {JsonConvert.SerializeObject(actualFromQueryWithoutContinutionTokens)}");

                        // Can't assert for reasons mentioned above
                        //Assert.IsTrue(
                        //    expected.SequenceEqual(actualFromQueryWithContinutionTokens, DistinctMapTests.JsonTokenEqualityComparer.Value),
                        //    $@" queryWithContinuations: {query},
                        //    expected:{JsonConvert.SerializeObject(expected)},
                        //    actual: {JsonConvert.SerializeObject(actualFromQueryWithContinutionTokens)}");
                    }
                }
            }
            catch (Exception ex)
            {
                if (expectedExceptionHandler != null)
                {
                    expectedExceptionHandler(ex);
                }
                else
                {
                    throw;
                }
            }
        }

        [TestMethod]
        public async Task TestQueryCrossPartitionTopOrderBy()
        {
            int seed = (int)(DateTime.UtcNow - new DateTime(1970, 1, 1)).TotalSeconds;
            uint numberOfDocuments = 1000;
            string partitionKey = "field_0";

            QueryOracle.QueryOracleUtil util = new QueryOracle.QueryOracle2(seed);
            IEnumerable<string> documents = util.GetDocuments(numberOfDocuments);

            await this.CreateIngestQueryDelete<string>(
                ConnectionModes.Direct | ConnectionModes.Gateway,
                documents,
                this.TestQueryCrossPartitionTopOrderByHelper,
                partitionKey,
                "/" + partitionKey);
        }

        private async Task TestQueryCrossPartitionTopOrderByHelper(CosmosContainer container, IEnumerable<Document> documents, string testArg)
        {
            string partitionKey = testArg;
            IDictionary<string, string> idToRangeMinKeyMap = new Dictionary<string, string>();
            IRoutingMapProvider routingMapProvider = await this.Client.DocumentClient.GetPartitionKeyRangeCacheAsync();

            CosmosContainerSettings containerSettings = await container.ReadAsync();
            foreach (Document document in documents)
            {
                IReadOnlyList<PartitionKeyRange> targetRanges = await routingMapProvider.TryGetOverlappingRangesAsync(
                containerSettings.ResourceId,
                Range<string>.GetPointRange(
                    PartitionKeyInternal.FromObjectArray(
                        new object[]
                        {
                            document.GetValue<int>(partitionKey)
                        },
                        true).GetEffectivePartitionKeyString(containerSettings.PartitionKey)));
                Debug.Assert(targetRanges.Count == 1);
                idToRangeMinKeyMap.Add(document.Id, targetRanges[0].MinInclusive);
            }

            IList<int> partitionKeyValues = new HashSet<int>(documents.Select(doc => doc.GetValue<int>(partitionKey))).ToList();

            // Test Empty Results
            List<string> expectedResults = new List<string> { };
            List<string> computedResults = new List<string>();

            string emptyQueryText = @"SELECT TOP 5 * FROM Root r WHERE r.partitionKey = 9991123 OR r.partitionKey = 9991124 OR r.partitionKey = 99991125";
            FeedOptions feedOptionsEmptyResult = new FeedOptions
            {
                EnableCrossPartitionQuery = true
            };

            List<Document> queryEmptyResult = await this.RunQuery<Document>(
                container,
                emptyQueryText,
                maxConcurrency: 1);

            computedResults = queryEmptyResult.Select(doc => doc.Id).ToList();
            computedResults.Sort();
            expectedResults.Sort();

            Random rand = new Random();
            Assert.AreEqual(string.Join(",", expectedResults), string.Join(",", computedResults));
            List<Task> tasks = new List<Task>();
            for (int trial = 0; trial < 1; ++trial)
            {
                foreach (bool fanOut in new[] { true, false })
                {
                    foreach (bool isParametrized in new[] { true, false })
                    {
                        foreach (bool hasTop in new[] { false, true })
                        {
                            foreach (bool hasOrderBy in new[] { false, true })
                            {
                                foreach (string sortOrder in new[] { string.Empty, "ASC", "DESC" })
                                {
                                    #region Expected Documents
                                    string topValueName = "@topValue";
                                    int top = rand.Next(4) * rand.Next(partitionKeyValues.Count);
                                    string queryText;
                                    string orderByField = "field_" + rand.Next(10);
                                    IEnumerable<Document> filteredDocuments;

                                    Func<string> getTop = () =>
                                        hasTop ? string.Format(CultureInfo.InvariantCulture, "TOP {0} ", isParametrized ? topValueName : top.ToString()) : string.Empty;

                                    Func<string> getOrderBy = () =>
                                        hasOrderBy ? string.Format(CultureInfo.InvariantCulture, " ORDER BY r.{0} {1}", orderByField, sortOrder) : string.Empty;

                                    if (fanOut)
                                    {
                                        queryText = string.Format(
                                            CultureInfo.InvariantCulture,
                                            "SELECT {0}r.id, r.{1} FROM r{2}",
                                            getTop(),
                                            partitionKey,
                                            getOrderBy());

                                        filteredDocuments = documents;
                                    }
                                    else
                                    {
                                        HashSet<int> selectedPartitionKeyValues = new HashSet<int>(partitionKeyValues
                                            .OrderBy(x => rand.Next())
                                            .ThenBy(x => x)
                                            .Take(rand.Next(1, Math.Min(100, partitionKeyValues.Count) + 1)));

                                        queryText = string.Format(
                                            CultureInfo.InvariantCulture,
                                            "SELECT {0}r.id, r.{1} FROM r WHERE r.{2} IN ({3}){4}",
                                            getTop(),
                                            partitionKey,
                                            partitionKey,
                                            string.Join(", ", selectedPartitionKeyValues),
                                            getOrderBy());

                                        filteredDocuments = documents
                                            .AsParallel()
                                            .Where(doc => selectedPartitionKeyValues.Contains(doc.GetValue<int>(partitionKey)));
                                    }

                                    if (hasOrderBy)
                                    {
                                        switch (sortOrder)
                                        {
                                            case "":
                                            case "ASC":
                                                filteredDocuments = filteredDocuments
                                                    .AsParallel()
                                                    .OrderBy(doc => doc.GetValue<int>(orderByField))
                                                    .ThenBy(doc => idToRangeMinKeyMap[doc.Id])
                                                    .ThenBy(doc => int.Parse(doc.Id, CultureInfo.InvariantCulture));
                                                break;
                                            case "DESC":
                                                filteredDocuments = filteredDocuments
                                                    .AsParallel()
                                                    .OrderByDescending(doc => doc.GetValue<int>(orderByField))
                                                    .ThenBy(doc => idToRangeMinKeyMap[doc.Id])
                                                    .ThenByDescending(doc => int.Parse(doc.Id, CultureInfo.InvariantCulture));
                                                break;
                                        }
                                    }
                                    else
                                    {
                                        filteredDocuments = filteredDocuments
                                            .AsParallel()
                                            .OrderBy(doc => idToRangeMinKeyMap[doc.Id])
                                            .ThenBy(doc => int.Parse(doc.Id, CultureInfo.InvariantCulture));
                                    }

                                    if (hasTop)
                                    {
                                        filteredDocuments = filteredDocuments.Take(top);
                                    }
                                    #endregion
                                    #region Actual Documents
                                    IEnumerable<Document> actualDocuments;

                                    int maxDegreeOfParallelism = hasTop ? rand.Next(4) : (rand.Next(2) == 0 ? -1 : (1 + rand.Next(0, 10)));
                                    int? maxItemCount = rand.Next(2) == 0 ? -1 : rand.Next(1, documents.Count());
                                    CosmosQueryRequestOptions feedOptions = new CosmosQueryRequestOptions
                                    {
                                        MaxBufferedItemCount = rand.Next(2) == 0 ? -1 : rand.Next(Math.Min(100, documents.Count()), documents.Count() + 1),
                                    };

                                    if (rand.Next(3) == 0)
                                    {
                                        maxItemCount = null;
                                    }

                                    CosmosSqlQueryDefinition querySpec = new CosmosSqlQueryDefinition(queryText);
                                    SqlParameterCollection parameters = new SqlParameterCollection();
                                    if (isParametrized)
                                    {
                                        if (hasTop)
                                        {
                                            querySpec.UseParameter(topValueName, top);
                                        }
                                    }

                                    DateTime startTime = DateTime.Now;
                                    List<Document> result = new List<Document>();
                                    CosmosResultSetIterator<Document> query = container.Items.CreateItemQuery<Document>(
                                        querySpec,
                                        maxConcurrency: maxDegreeOfParallelism,
                                        requestOptions: feedOptions);

                                    while (query.HasMoreResults)
                                    {
                                        CosmosQueryResponse<Document> response = await query.FetchNextSetAsync();
                                        result.AddRange(response);
                                    }

                                    actualDocuments = result;

                                    #endregion

                                    double time = (DateTime.Now - startTime).TotalMilliseconds;

                                    Trace.TraceInformation("<Query>: {0}, <Document Count>: {1}, <MaxItemCount>: {2}, <MaxDegreeOfParallelism>: {3}, <MaxBufferedItemCount>: {4}, <Time>: {5} ms",
                                        JsonConvert.SerializeObject(querySpec),
                                        actualDocuments.Count(),
                                        maxItemCount,
                                        maxDegreeOfParallelism,
                                        feedOptions.MaxBufferedItemCount,
                                        time);

                                    string allDocs = JsonConvert.SerializeObject(documents);

                                    string expectedResultDocs = JsonConvert.SerializeObject(filteredDocuments);
                                    IEnumerable<string> expectedResult = filteredDocuments.Select(doc => doc.Id);

                                    string actualResultDocs = JsonConvert.SerializeObject(actualDocuments);
                                    IEnumerable<string> actualResult = actualDocuments.Select(doc => doc.Id);

                                    Assert.AreEqual(
                                        string.Join(", ", expectedResult),
                                        string.Join(", ", actualResult),
                                        $"query: {querySpec}, trial: {trial}, fanOut: {fanOut}, hasTop: {hasTop}, hasOrderBy: {hasOrderBy}, sortOrder: {sortOrder}");
                                }
                            }
                        }
                    }
                }
            }
        }

        [TestMethod]
        public async Task TestQueryCrossPartitionTop()
        {
            int seed = (int)(DateTime.UtcNow - new DateTime(1970, 1, 1)).TotalSeconds;
            uint numberOfDocuments = 100;
            string partitionKey = "field_0";

            QueryOracle.QueryOracleUtil util = new QueryOracle.QueryOracle2(seed);
            IEnumerable<string> documents = util.GetDocuments(numberOfDocuments);

            await this.CreateIngestQueryDelete(
                ConnectionModes.Direct,
                documents,
                this.TestQueryCrossPartitionTopHelper,
                "/" + partitionKey);
        }

        private async Task TestQueryCrossPartitionTopHelper(CosmosContainer container, IEnumerable<Document> documents)
        {
            List<string> queryFormats = new List<string>()
            {
                "SELECT {0} TOP {1} * FROM c",
                // Can't do order by since order by needs to look at all partitions before returning a single document =>
                // thus we can't tell how many documents the SDK needs to recieve.
                //"SELECT {0} TOP {1} * FROM c ORDER BY c._ts",

                // Can't do aggregates since that also retrieves more documents than the user sees
                //"SELECT {0} TOP {1} VALUE AVG(c._ts) FROM c",
            };

            foreach (string queryFormat in queryFormats)
            {
                foreach (bool useDistinct in new bool[] { true, false })
                {
                    foreach (int topCount in new int[] { 0, 1, 10 })
                    {
                        foreach (int pageSize in new int[] { 1, 10 })
                        {
                            // Run the query and use the query metrics to make sure the query didn't grab more documents
                            // than needed.

                            string query = string.Format(queryFormat, useDistinct ? "DISTINCT" : string.Empty, topCount);
                            FeedOptions feedOptions = new FeedOptions
                            {
                                MaxBufferedItemCount = 1000,

                            };

                            // Max DOP needs to be 0 since the query needs to run in serial => 
                            // otherwise the parallel code will prefetch from other partitions,
                            // since the first N-1 partitions might be empty.
                            CosmosResultSetIterator<dynamic> documentQuery = container.Items.CreateItemQuery<dynamic>(
                                    query,
                                    maxConcurrency: 0,
                                    maxItemCount: pageSize);

                            //QueryMetrics aggregatedQueryMetrics = QueryMetrics.Zero;
                            int numberOfDocuments = 0;
                            while (documentQuery.HasMoreResults)
                            {
                                CosmosQueryResponse<dynamic> cosmosQueryResponse = await documentQuery.FetchNextSetAsync();

                                numberOfDocuments += cosmosQueryResponse.Count();
                                //foreach (QueryMetrics queryMetrics in cosmosQueryResponse.QueryMetrics.Values)
                                //{
                                //    aggregatedQueryMetrics += queryMetrics;
                                //}
                            }

                            Assert.IsTrue(
                                numberOfDocuments <= topCount,
                                $"Received {numberOfDocuments} documents with query: {query} and pageSize: {pageSize}");
                            //if (!useDistinct)
                            //{
                            //    Assert.IsTrue(
                            //        aggregatedQueryMetrics.OutputDocumentCount <= topCount,
                            //        $"Received {aggregatedQueryMetrics.OutputDocumentCount} documents query: {query} and pageSize: {pageSize}");
                            //}
                        }
                    }
                }
            }
        }

        private struct CrossPartitionWithContinuationsArgs
        {
            public int NumberOfDocuments;
            public string PartitionKey;
            public string NumberField;
            public string BoolField;
            public string StringField;
            public string NullField;
            public string Children;
        }

        [Ignore]
        [TestCategory("Quarantine")] //until serviceInterop enabled again
        [TestMethod]
        public async Task TestQueryCrossPartitionWithContinuations()
        {
            int numberOfDocuments = 1 << 2;
            string partitionKey = "key";
            string numberField = "numberField";
            string boolField = "boolField";
            string stringField = "stringField";
            string nullField = "nullField";
            string children = "children";

            List<string> documents = new List<string>(numberOfDocuments);
            for (int i = 0; i < numberOfDocuments; ++i)
            {
                Document doc = new Document();
                doc.SetPropertyValue(partitionKey, i);
                doc.SetPropertyValue(numberField, i % 8);
                doc.SetPropertyValue(boolField, (i % 2) == 0 ? bool.TrueString : bool.FalseString);
                doc.SetPropertyValue(stringField, (i % 8).ToString());
                doc.SetPropertyValue(nullField, null);
                doc.SetPropertyValue(children, new[] { i % 2, i % 2, i % 3, i % 3, i });
                documents.Add(doc.ToString());
            }

            CrossPartitionWithContinuationsArgs args = new CrossPartitionWithContinuationsArgs()
            {
                NumberOfDocuments = numberOfDocuments,
                PartitionKey = partitionKey,
                NumberField = numberField,
                BoolField = boolField,
                StringField = stringField,
                NullField = nullField,
                Children = children,
            };

            await this.CreateIngestQueryDelete<CrossPartitionWithContinuationsArgs>(
                ConnectionModes.Direct | ConnectionModes.Gateway,
                documents,
                this.TestQueryCrossPartitionWithContinuationsHelper,
                args,
                "/" + partitionKey);
        }

        private async Task TestQueryCrossPartitionWithContinuationsHelper(CosmosContainer container, IEnumerable<Document> documents, CrossPartitionWithContinuationsArgs args)
        {
            int documentCount = args.NumberOfDocuments;
            string partitionKey = args.PartitionKey;
            string numberField = args.NumberField;
            string boolField = args.BoolField;
            string stringField = args.StringField;
            string nullField = args.NullField;
            string children = args.Children;

            // Try resuming from bad continuation token
            #region BadContinuations
            try
            {
                await container.Items.CreateItemQuery<Document>(
                    "SELECT * FROM t",
                    maxConcurrency: 1,
                    continuationToken: Guid.NewGuid().ToString()).FetchNextSetAsync();

                Assert.Fail("Expect exception");
            }
            catch (CosmosException dce)
            {
                Assert.IsTrue(dce.StatusCode == HttpStatusCode.BadRequest);
            }

            try
            {
                await container.Items.CreateItemQuery<Document>(
                    "SELECT TOP 10 * FROM r",
                    maxConcurrency: -1,
                    maxItemCount: 10,
                    continuationToken: "{'top':11}").FetchNextSetAsync();

                Assert.Fail("Expect exception");
            }
            catch (CosmosException dce)
            {
                Assert.IsTrue(dce.StatusCode == HttpStatusCode.BadRequest);
            }

            try
            {
                await container.Items.CreateItemQuery<Document>(
                    "SELECT * FROM r ORDER BY r.field1",
                    maxConcurrency: -1,
                    maxItemCount: 10,
                    continuationToken: "{'compositeToken':{'range':{'min':'05C1E9CD673398','max':'FF'}}, 'orderByItems':[{'item':2}, {'item':1}]}").FetchNextSetAsync();

                Assert.Fail("Expect exception");
            }
            catch (CosmosException dce)
            {
                Assert.IsTrue(dce.StatusCode == HttpStatusCode.BadRequest);
            }

            try
            {
                await container.Items.CreateItemQuery<Document>(
                   "SELECT * FROM r ORDER BY r.field1, r.field2",
                   maxConcurrency: -1,
                   maxItemCount: 10,
                   continuationToken: "{'compositeToken':{'range':{'min':'05C1E9CD673398','max':'FF'}}, 'orderByItems':[{'item':2}, {'item':1}]}").FetchNextSetAsync();

                Assert.Fail("Expect exception");
            }
            catch (CosmosException dce)
            {
                Assert.IsTrue(dce.StatusCode == HttpStatusCode.BadRequest);
            }
            #endregion

            CosmosQueryResponse<Document> responseWithEmptyContinuationExpected = await container.Items.CreateItemQuery<Document>(
                string.Format(CultureInfo.InvariantCulture, "SELECT TOP 1 * FROM r ORDER BY r.{0}", partitionKey),
                    maxConcurrency: 10,
                    maxItemCount: -1).FetchNextSetAsync();

            Assert.AreEqual(null, responseWithEmptyContinuationExpected.ContinuationToken);

            string[] queries = new[]
            {
                $"SELECT * FROM r",
                $"SELECT * FROM r WHERE r.{partitionKey} BETWEEN 0 AND {documentCount}",
                $"SELECT r.{partitionKey} FROM r JOIN c in r.{children}",
                $"SELECT * FROM r ORDER BY r.{partitionKey}",
                $"SELECT * FROM r WHERE r.{partitionKey} BETWEEN 0 AND {documentCount} ORDER BY r.{numberField} DESC",
                $"SELECT r.{partitionKey} FROM r JOIN c in r.{children} ORDER BY r.{numberField}",
                $"SELECT TOP 10 * FROM r",
                $"SELECT TOP 10 * FROM r WHERE r.{partitionKey} BETWEEN 0 AND {documentCount} ORDER BY r.{partitionKey} DESC",
                $"SELECT TOP 10 * FROM r ORDER BY r.{numberField}",
                $"SELECT TOP 40 r.{partitionKey} FROM r JOIN c in r.{children} ORDER BY r.{numberField} DESC",
                $"SELECT * FROM r WHERE r.{partitionKey} BETWEEN 0 AND {documentCount} ORDER BY r.{boolField} DESC",
                $"SELECT * FROM r WHERE r.{partitionKey} BETWEEN 0 AND {documentCount} ORDER BY r.{stringField} DESC",
                $"SELECT * FROM r WHERE r.{partitionKey} BETWEEN 0 AND {documentCount} ORDER BY r.{nullField} DESC",
            };

            foreach (string query in queries)
            {
                List<Document> expectedValues = await this.RunQuery<Document>(
                    container,
                    query,
                    maxConcurrency: 0);

                foreach (int pageSize in new int[] { 1, documentCount / 2, documentCount })
                {
                    List<Document> retrievedDocuments = new List<Document>();

                    CosmosResultSetIterator<Document> documentQuery;
                    string continuationToken = default(string);
                    bool hasMoreResults;

                    do
                    {
                        CosmosQueryRequestOptions feedOptions = new CosmosQueryRequestOptions
                        {
                            MaxBufferedItemCount = 10000,
                        };

                        documentQuery = container.Items.CreateItemQuery<Document>(
                            query,
                            maxConcurrency: 10000,
                            maxItemCount: pageSize,
                            continuationToken: continuationToken,
                            requestOptions: feedOptions);

                        CosmosQueryResponse<Document> response;
                        try
                        {
                            response = await documentQuery.FetchNextSetAsync();
                        }
                        catch (Exception ex)
                        {
                            throw ex;
                        }

                        Assert.IsTrue(
                            response.Count() <= pageSize,
                            string.Format(
                            CultureInfo.InvariantCulture,
                            "Actual result count {0} should be less or equal to requested page size {1}. Query: {2}, Continuation: {3}, Results.Count: {4}",
                            response.Count(),
                            pageSize,
                            query,
                            continuationToken,
                            retrievedDocuments.Count));
                        continuationToken = response.ContinuationToken;
                        retrievedDocuments.AddRange(response);

                        hasMoreResults = documentQuery.HasMoreResults;
                    } while (hasMoreResults);

                    Assert.AreEqual(
                        string.Join(", ", expectedValues.Select(doc => doc.GetPropertyValue<int>(partitionKey))),
                        string.Join(", ", retrievedDocuments.Select(doc => doc.GetPropertyValue<int>(partitionKey))),
                        string.Format(CultureInfo.InvariantCulture, "query: {0}, page size: {1}", query, pageSize));
                }
            }
        }

        [TestMethod]
        public async Task TestMultiOrderByQueries()
        {
            int numberOfDocuments = 4;

            List<string> documents = new List<string>(numberOfDocuments);
            Random random = new Random(1234);
            for (int i = 0; i < numberOfDocuments; ++i)
            {
                MultiOrderByDocument multiOrderByDocument = CrossPartitionQueryTests.GenerateMultiOrderByDocument(random);
                int numberOfDuplicates = 5;

                for (int j = 0; j < numberOfDuplicates; j++)
                {
                    // Add the document itself for exact duplicates
                    documents.Add(JsonConvert.SerializeObject(multiOrderByDocument));

                    // Permute all the fields so that there are duplicates with tie breaks
                    MultiOrderByDocument numberClone = MultiOrderByDocument.GetClone(multiOrderByDocument);
                    numberClone.NumberField = random.Next(0, 5);
                    documents.Add(JsonConvert.SerializeObject(numberClone));

                    MultiOrderByDocument stringClone = MultiOrderByDocument.GetClone(multiOrderByDocument);
                    stringClone.StringField = random.Next(0, 5).ToString();
                    documents.Add(JsonConvert.SerializeObject(stringClone));

                    MultiOrderByDocument boolClone = MultiOrderByDocument.GetClone(multiOrderByDocument);
                    boolClone.BoolField = random.Next(0, 2) % 2 == 0;
                    documents.Add(JsonConvert.SerializeObject(boolClone));

                    // Also fuzz what partition it goes to
                    MultiOrderByDocument partitionClone = MultiOrderByDocument.GetClone(multiOrderByDocument);
                    partitionClone.PartitionKey = random.Next(0, 5);
                    documents.Add(JsonConvert.SerializeObject(partitionClone));
                }
            }

            Cosmos.IndexingPolicy indexingPolicy = new Cosmos.IndexingPolicy()
            {
                CompositeIndexes = new Collection<Collection<Cosmos.CompositePath>>()
                {
                    // Simple
                    new Collection<Cosmos.CompositePath>()
                    {
                        new Cosmos.CompositePath()
                        {
                            Path = "/" + nameof(MultiOrderByDocument.NumberField),
                            Order = Cosmos.CompositePathSortOrder.Ascending,
                        },
                        new Cosmos.CompositePath()
                        {
                            Path = "/" + nameof(MultiOrderByDocument.StringField),
                            Order = Cosmos.CompositePathSortOrder.Descending,
                        }
                    },

                    // Max Columns
                    new Collection<Cosmos.CompositePath>()
                    {
                        new Cosmos.CompositePath()
                        {
                            Path = "/" + nameof(MultiOrderByDocument.NumberField),
                            Order = Cosmos.CompositePathSortOrder.Descending,
                        },
                        new Cosmos.CompositePath()
                        {
                            Path = "/" + nameof(MultiOrderByDocument.StringField),
                            Order = Cosmos.CompositePathSortOrder.Ascending,
                        },
                        new Cosmos.CompositePath()
                        {
                            Path = "/" + nameof(MultiOrderByDocument.NumberField2),
                            Order = Cosmos.CompositePathSortOrder.Descending,
                        },
                        new Cosmos.CompositePath()
                        {
                            Path = "/" + nameof(MultiOrderByDocument.StringField2),
                            Order = Cosmos.CompositePathSortOrder.Ascending,
                        }
                    },

                    // All primitive values
                    new Collection<Cosmos.CompositePath>()
                    {
                        new Cosmos.CompositePath()
                        {
                            Path = "/" + nameof(MultiOrderByDocument.NumberField),
                            Order = Cosmos.CompositePathSortOrder.Descending,
                        },
                        new Cosmos.CompositePath()
                        {
                            Path = "/" + nameof(MultiOrderByDocument.StringField),
                            Order = Cosmos.CompositePathSortOrder.Ascending,
                        },
                        new Cosmos.CompositePath()
                        {
                            Path = "/" + nameof(MultiOrderByDocument.BoolField),
                            Order = Cosmos.CompositePathSortOrder.Descending,
                        },
                        new Cosmos.CompositePath()
                        {
                            Path = "/" + nameof(MultiOrderByDocument.NullField),
                            Order = Cosmos.CompositePathSortOrder.Ascending,
                        }
                    },

                    // Primitive and Non Primitive (waiting for composite on objects and arrays)
                    //new Collection<Cosmos.CompositePath>()
                    //{
                    //    new Cosmos.CompositePath()
                    //    {
                    //        Path = "/" + nameof(MultiOrderByDocument.NumberField),
                    //    },
                    //    new Cosmos.CompositePath()
                    //    {
                    //        Path = "/" + nameof(MultiOrderByDocument.ObjectField),
                    //    },
                    //    new Cosmos.CompositePath()
                    //    {
                    //        Path = "/" + nameof(MultiOrderByDocument.StringField),
                    //    },
                    //    new Cosmos.CompositePath()
                    //    {
                    //        Path = "/" + nameof(MultiOrderByDocument.ArrayField),
                    //    },
                    //},

                    // Long strings
                    new Collection<Cosmos.CompositePath>()
                    {
                        new Cosmos.CompositePath()
                        {
                            Path = "/" + nameof(MultiOrderByDocument.StringField),
                        },
                        new Cosmos.CompositePath()
                        {
                            Path = "/" + nameof(MultiOrderByDocument.ShortStringField),
                        },
                        new Cosmos.CompositePath()
                        {
                            Path = "/" + nameof(MultiOrderByDocument.MediumStringField),
                        },
                        new Cosmos.CompositePath()
                        {
                            Path = "/" + nameof(MultiOrderByDocument.LongStringField),
                        }
                    },

                    // System Properties 
                    //new Collection<Cosmos.CompositePath>()
                    //{
                    //    new Cosmos.CompositePath()
                    //    {
                    //        Path = "/id",
                    //    },
                    //    new Cosmos.CompositePath()
                    //    {
                    //        Path = "/_ts",
                    //    },
                    //    new Cosmos.CompositePath()
                    //    {
                    //        Path = "/_etag",
                    //    },

                    //    // _rid is not allowed
                    //    //new Cosmos.CompositePath()
                    //    //{
                    //    //    Path = "/_rid",
                    //    //},
                    //},
                }
            };

            await this.RunWithApiVersion(
                HttpConstants.Versions.v2018_09_17,
                async () =>
                {
                    await this.CreateIngestQueryDelete(
                        ConnectionModes.Direct,
                        documents,
                        this.TestMultiOrderByQueriesHelper,
                        "/" + nameof(MultiOrderByDocument.PartitionKey),
                        indexingPolicy,
                        this.CreateNewCosmosClient);
                });
        }

        private sealed class MultiOrderByDocument
        {
            public double NumberField { get; set; }
            public double NumberField2 { get; set; }
            public bool BoolField { get; set; }
            public string StringField { get; set; }
            public string StringField2 { get; set; }
            public object NullField { get; set; }
            public object ObjectField { get; set; }
            public List<object> ArrayField { get; set; }
            public string ShortStringField { get; set; }
            public string MediumStringField { get; set; }
            public string LongStringField { get; set; }
            public int PartitionKey { get; set; }

            public static MultiOrderByDocument GetClone(MultiOrderByDocument other)
            {
                return JsonConvert.DeserializeObject<MultiOrderByDocument>(JsonConvert.SerializeObject(other));
            }
        }

        private static MultiOrderByDocument GenerateMultiOrderByDocument(Random random)
        {
            return new MultiOrderByDocument()
            {
                NumberField = random.Next(0, 5),
                NumberField2 = random.Next(0, 5),
                BoolField = (random.Next() % 2) == 0,
                StringField = random.Next(0, 5).ToString(),
                StringField2 = random.Next(0, 5).ToString(),
                NullField = null,
                ObjectField = new object(),
                ArrayField = new List<object>(),
                ShortStringField = new string('a', random.Next(0, 100)),
                MediumStringField = new string('a', random.Next(100, 128)),
                //Max precisions is 2kb / number of terms
                LongStringField = new string('a', random.Next(128, 255)),
                PartitionKey = random.Next(0, 5),
            };
        }

        private async Task TestMultiOrderByQueriesHelper(
            CosmosContainer container,
            IEnumerable<Document> documents)
        {
            CosmosContainerSettings containerSettings = await container.ReadAsync();
            // For every composite index
            foreach (Collection<Cosmos.CompositePath> compositeIndex in containerSettings.IndexingPolicy.CompositeIndexes)
            {
                // for every order
                foreach (bool invert in new bool[] { false, true })
                {
                    foreach (bool hasTop in new bool[] { false, true })
                    {
                        foreach (bool hasFilter in new bool[] { false, true })
                        {
                            // Generate a multi order by from that index
                            List<string> orderByItems = new List<string>();
                            List<string> selectItems = new List<string>();
                            bool isDesc;
                            foreach (Cosmos.CompositePath compositePath in compositeIndex)
                            {
                                isDesc = compositePath.Order == Cosmos.CompositePathSortOrder.Descending ? true : false;
                                if (invert)
                                {
                                    isDesc = !isDesc;
                                }

                                string isDescString = isDesc ? "DESC" : "ASC";
                                orderByItems.Add($"root.{compositePath.Path.Replace("/", "")} { isDescString }");
                                selectItems.Add($"root.{compositePath.Path.Replace("/", "")}");
                            }

                            const int topCount = 10;
                            string topString = hasTop ? $"TOP {topCount}" : string.Empty;
                            string whereString = hasFilter ? $"WHERE root.{nameof(MultiOrderByDocument.NumberField)} % 2 = 0" : string.Empty;
                            string query = $@"
                                    SELECT { topString } VALUE [{string.Join(", ", selectItems)}] 
                                    FROM root { whereString }
                                    ORDER BY {string.Join(", ", orderByItems)}";
#if false
                            // Used for debugging which partitions have which documents
                            IReadOnlyList<PartitionKeyRange> pkranges = GetPartitionKeyRanges(container);
                            foreach (PartitionKeyRange pkrange in pkranges)
                            {
                                List<dynamic> documentsWithinPartition = cosmosClient.CreateDocumentQuery(
                                    container,
                                    query,
                                    new FeedOptions()
                                    {
                                        EnableScanInQuery = true,
                                        PartitionKeyRangeId = pkrange.Id
                                    }).ToList();
                            }
#endif

                            #region ExpectedUsingLinq
                            List<MultiOrderByDocument> castedDocuments = documents
                                .Select(x => JsonConvert.DeserializeObject<MultiOrderByDocument>(JsonConvert.SerializeObject(x)))
                                .ToList();

                            if (hasFilter)
                            {
                                castedDocuments = castedDocuments.Where(document => document.NumberField % 2 == 0).ToList();
                            }

                            IOrderedEnumerable<MultiOrderByDocument> oracle;
                            Cosmos.CompositePath firstCompositeIndex = compositeIndex.First();

                            isDesc = firstCompositeIndex.Order == Cosmos.CompositePathSortOrder.Descending ? true : false;
                            if (invert)
                            {
                                isDesc = !isDesc;
                            }

                            if (isDesc)
                            {
                                oracle = castedDocuments.OrderByDescending(x => x.GetType().GetProperty(firstCompositeIndex.Path.Replace("/", "")).GetValue(x, null));
                            }
                            else
                            {
                                oracle = castedDocuments.OrderBy(x => x.GetType().GetProperty(firstCompositeIndex.Path.Replace("/", "")).GetValue(x, null));
                            }

                            foreach (Cosmos.CompositePath compositePath in compositeIndex.Skip(1))
                            {
                                isDesc = compositePath.Order == Cosmos.CompositePathSortOrder.Descending ? true : false;
                                if (invert)
                                {
                                    isDesc = !isDesc;
                                }

                                if (isDesc)
                                {
                                    oracle = oracle.ThenByDescending(x => x.GetType().GetProperty(compositePath.Path.Replace("/", "")).GetValue(x, null));
                                }
                                else
                                {
                                    oracle = oracle.ThenBy(x => x.GetType().GetProperty(compositePath.Path.Replace("/", "")).GetValue(x, null));
                                }
                            }

                            List<List<object>> expected = new List<List<object>>();
                            foreach (MultiOrderByDocument document in oracle)
                            {
                                List<object> projectedItems = new List<object>();
                                foreach (Cosmos.CompositePath compositePath in compositeIndex)
                                {
                                    projectedItems.Add(typeof(MultiOrderByDocument).GetProperty(compositePath.Path.Replace("/", "")).GetValue(document, null));
                                }

                                expected.Add(projectedItems);
                            }

                            if (hasTop)
                            {
                                expected = expected.Take(topCount).ToList();
                            }

                            #endregion

                            CosmosQueryRequestOptions feedOptions = new CosmosQueryRequestOptions()
                            {
                                MaxBufferedItemCount = 1000,
                            };

                            List<List<object>> actualFromQueryWithoutContinutionToken = await this.RunQuery<List<object>>(
                                container,
                                query,
                                maxItemCount: 3,
                                maxConcurrency: 10,
                                requestOptions: feedOptions);
                            this.AssertMultiOrderByResults(expected, actualFromQueryWithoutContinutionToken, query + "(without continuations)");
                        }
                    }
                }
            }
        }

        private void AssertMultiOrderByResults(List<List<object>> expected, List<List<object>> actual, string query)
        {
            IEnumerable<Tuple<List<JToken>, List<JToken>>> expectedZippedWithActual = expected
                .Zip(actual, (first, second) =>
                new Tuple<List<JToken>, List<JToken>>(
                    first.Select(x => x == null ? null : JToken.FromObject(x)).ToList(),
                    second.Select(x => x == null ? null : JToken.FromObject(x)).ToList()));

            foreach (Tuple<List<JToken>, List<JToken>> expectedAndActual in expectedZippedWithActual)
            {
                List<JToken> first = expectedAndActual.Item1;
                List<JToken> second = expectedAndActual.Item2;
                Assert.IsTrue(
                    first.SequenceEqual(second, JsonTokenEqualityComparer.Value),
                    $@"
                        query: {query}: 
                        first: {JsonConvert.SerializeObject(first)}
                        second: {JsonConvert.SerializeObject(second)}
                        expected: {JsonConvert.SerializeObject(expected).Replace(".0", "")}
                        actual: {JsonConvert.SerializeObject(actual).Replace(".0", "")}");
            }
        }

        private sealed class Headers
        {
            public double TotalRUs { get; set; }
            public long NumberOfDocuments { get; set; }
            public long RetrievedDocumentCount { get; set; }
            public long RetrievedDocumentSize { get; set; }
            public long OutputDocumentCount { get; set; }
            public long OutputDocumentSize { get; set; }

            public override bool Equals(object obj)
            {
                Headers headers = obj as Headers;
                if (headers != null)
                {
                    return Headers.Equals(this, headers);
                }
                else
                {
                    return false;
                }
            }

            public override int GetHashCode()
            {
                return 0;
            }

            private static bool Equals(Headers headers1, Headers headers2)
            {
                return Math.Abs(headers1.TotalRUs - headers2.TotalRUs) < 10E-4 &&
                    headers1.NumberOfDocuments == headers2.NumberOfDocuments &&
                    headers1.RetrievedDocumentCount == headers2.RetrievedDocumentCount &&
                    headers1.RetrievedDocumentSize == headers2.RetrievedDocumentSize &&
                    headers1.OutputDocumentCount == headers2.OutputDocumentCount &&
                    headers1.OutputDocumentSize == headers2.OutputDocumentSize;
            }
        }

        /// <summary>
        /// Tests CosmosQueryResponse.ResponseLengthInBytes is populated with the correct value for queries on Direct connection.
        /// The expected response length is determined by capturing DocumentServiceResponse events and aggregate their lengths.
        /// Queries covered are standard/Top/Aggregate/Distinct and use MaxItemCount to force smaller page sizes, Max DOP and MaxBufferedItems to
        /// validate producer query threads are handled properly. Note: TOP has known non-deterministic behavior for non-zero Max DOP, so the setting
        /// is set to zero to avoid these cases.
        /// </summary>
        /// <returns></returns>
        //[TestCategory("Quarantine")] //until serviceInterop enabled again
        //[Ignore]
        //[TestMethod]
        //public async Task TestResponseLengthOverMultiplePartitions()
        //{
        //    EventHandler<ReceivedResponseEventArgs> responseHandler = DocumentResponseLengthHandler;

        //    int seed = (int)(DateTime.UtcNow - new DateTime(1970, 1, 1)).TotalSeconds;
        //    uint numberOfDocuments = 100;
        //    string partitionKey = "field_0";

        //    QueryOracle.QueryOracleUtil util = new QueryOracle.QueryOracle2(seed);
        //    IEnumerable<string> documents = util.GetDocuments(numberOfDocuments);

        //    await this.CreateIngestQueryDelete(
        //        ConnectionModes.Direct,
        //        documents,
        //        this.ExceuteResponseLengthQueriesAndValidation,
        //        (connectionMode) =>
        //        {
        //            return TestCommon.CreateCosmosClient(
        //                useGateway: connectionMode == ConnectionMode.Gateway ? true : false,
        //                recievedResponseEventHandler: responseHandler);
        //        },
        //        partitionKey: "/" + partitionKey,
        //        testArgs: partitionKey);
        //}

        //private static void DocumentResponseLengthHandler(object sender, ReceivedResponseEventArgs e)
        //{
        //    if (!e.IsHttpResponse())
        //    {
        //        List<object> headerKeyValues = new List<object>();
        //        foreach (string key in e.DocumentServiceRequest.Headers)
        //        {
        //            headerKeyValues.Add(new { Key = key, Values = e.DocumentServiceRequest.Headers.GetValues(key)?.ToList() });
        //        }

        //        CrossPartitionQueryTests.responseLengthBytes.Value.IncrementBy(e.DocumentServiceResponse.ResponseBody.Length);
        //        Console.WriteLine("{0} : DocumentServiceResponse: Query {1}, OuterActivityId: {2}, Length: {3}, Request op type: {4}, resource type: {5}, continuation: {6}, headers: {7}",
        //            DateTime.UtcNow,
        //            e.DocumentServiceRequest.QueryString,
        //            CrossPartitionQueryTests.outerCosmosQueryResponseActivityId.Value,
        //            e.DocumentServiceResponse.ResponseBody.Length,
        //            e.DocumentServiceRequest.OperationType,
        //            e.DocumentServiceRequest.ResourceType,
        //            e.DocumentServiceRequest.Continuation,
        //            JsonConvert.SerializeObject(headerKeyValues));
        //    }
        //}

        //private async Task ExceuteResponseLengthQueriesAndValidation(CosmosContainer coll, IEnumerable<Document> documents, dynamic testArgs)
        //{
        //    string partitionKey = testArgs;

        //    await this.AssertResponseLength(queryClient, coll, "SELECT * FROM r");
        //    await this.AssertResponseLength(queryClient, coll, "SELECT VALUE COUNT(1) FROM c");
        //    await this.AssertResponseLength(queryClient, coll, "SELECT * FROM r", maxItemCount: 10);
        //    await this.AssertResponseLength(queryClient, coll, "SELECT * FROM r", maxItemCount: 10, maxBufferedCount: 100);
        //    await this.AssertResponseLength(queryClient, coll, "SELECT VALUE MAX(c._ts) FROM c", maxItemCount: 10);
        //    await this.AssertResponseLength(queryClient, coll, $"SELECT DISTINCT VALUE r.{partitionKey} FROM r", maxItemCount: 10);

        //    await this.AssertResponseLength(queryClient, coll, "SELECT TOP 5 * FROM c ORDER BY c._ts", isTopQuery: true);
        //    await this.AssertResponseLength(queryClient, coll, "SELECT TOP 32 * FROM r", isTopQuery: true, maxItemCount: 10);
        //}

        //private async Task AssertResponseLength(
        //    CosmosContainer coll,
        //    string query,
        //    bool isTopQuery = false,
        //    int maxItemCount = 1,
        //    int maxBufferedCount = -1,
        //    int maxReadItemCount = -1)
        //{
        //    long expectedResponseLength = 0;
        //    long actualResponseLength = 0;

        //    // NOTE: For queries with 'TOP' clause and non-zero Max DOP, it is possible for additional backend responses to return
        //    // after the target item limit has been reached and the final CosmosQueryResponse is being percolated to the caller. 
        //    // As a result, the stats from these responses will not be included in the aggregated results on the CosmosQueryResponses.
        //    // To avoid this non-determinism in the test cases, we force Max DOP to zero if the query is a 'top' query.
        //    FeedOptions feedOptions = new FeedOptions
        //    {
        //        EnableCrossPartitionQuery = true,
        //        MaxItemCount = maxItemCount,
        //        MaxDegreeOfParallelism = isTopQuery ? 0 : 50,
        //        MaxBufferedItemCount = isTopQuery ? 0 : maxBufferedCount,
        //    };

        //    this.responseLengthBytes.Value = new LocalCounter();
        //    this.outerCosmosQueryResponseActivityId.Value = Guid.NewGuid();

        //    Console.WriteLine("{0} : Running query: {1}, maxitemcount: {2}, maxBufferedCount: {3}, max read count: {4}, OuterActivityId: {5}",
        //        DateTime.UtcNow,
        //        query,
        //        maxItemCount,
        //        maxBufferedCount,
        //        maxReadItemCount,
        //        this.outerCosmosQueryResponseActivityId.Value);

        //    int totalReadCount = 0;

        //    CosmosResultSetIterator<dynamic> docQuery = coll.Items.CreateItemQuery<dynamic>(query, feedOptions);
        //        while (docQuery.HasMoreResults && (maxReadItemCount < 0 || maxReadItemCount > totalReadCount))
        //        {
        //            CosmosQueryResponse<dynamic> response = await docQuery.FetchNextSetAsync();

        //            Console.WriteLine("{0} : CosmosQueryResponse: Query: {1}, ActivityId: {2}, OuterActivityId: {3}, RequestCharge: {4}, ResponseLength: {5}, ItemCount: {6}",
        //                DateTime.UtcNow,
        //                query,
        //                response.ActivityId,
        //                this.outerCosmosQueryResponseActivityId.Value,
        //                response.RequestCharge,
        //                response.ResponseLengthBytes,
        //                response.Count);

        //            actualResponseLength += response.ResponseLengthBytes;
        //            totalReadCount += response.Count;
        //        }
        //    }

        //    expectedResponseLength = this.responseLengthBytes.Value.Value;
        //    Console.WriteLine("Completed query: {0}, response length: {1}, total item count: {2}, document service response length: {3}, OuterActivityId: {4}",
        //        query,
        //        actualResponseLength,
        //        totalReadCount,
        //        expectedResponseLength,
        //        this.outerCosmosQueryResponseActivityId.Value);

        //    Assert.AreNotEqual(0, expectedResponseLength);

        //    // Top queries don't necessarily return a response length that matches the DocumentServiceResponses.
        //    // To avoid the discrepancies, skip exact response length validation for these queries.
        //    // We still run the query to ensure there are no exceptions.
        //    if (!isTopQuery)
        //    {
        //        Assert.AreEqual(expectedResponseLength, actualResponseLength, "Aggregate CosmosQueryResponse length did not match document service response.");
        //    }

        //    this.responseLengthBytes.Value = null;
        //}

        private async Task<List<T>> RunQuery<T>(
            CosmosContainer container,
            string query,
            int maxConcurrency,
            int? maxItemCount = null,
            CosmosQueryRequestOptions requestOptions = null)
        {
            CosmosResultSetIterator<T> resultSetIterator = container.Items.CreateItemQuery<T>(
                query,
                maxConcurrency: maxConcurrency,
                maxItemCount: maxItemCount,
                requestOptions: requestOptions);

            List<T> items = new List<T>();
            while (resultSetIterator.HasMoreResults)
            {
                items.AddRange(await resultSetIterator.FetchNextSetAsync());
            }

            return items;
        }

        private async Task<List<T>> RunSinglePartitionQuery<T>(
            CosmosContainer container,
            string query,
            object partitionKey,
            int? maxItemCount = null,
            CosmosQueryRequestOptions requestOptions = null)
        {
            CosmosResultSetIterator<T> resultSetIterator = container.Items.CreateItemQuery<T>(
                query,
                partitionKey: partitionKey,
                maxItemCount: maxItemCount,
                requestOptions: requestOptions);

            List<T> items = new List<T>();
            while (resultSetIterator.HasMoreResults)
            {
                items.AddRange(await resultSetIterator.FetchNextSetAsync());
            }

            return items;
        }

        private class LocalCounter
        {
            private long value;

            public long Value => this.value;

            public long IncrementBy(long incrementBy)
            {
                return Interlocked.Add(ref this.value, incrementBy);
            }
        }

        internal sealed class MockDistinctMap
        {
            // using custom comparer, since newtonsoft thinks this:
            // JToken.DeepEquals(JToken.Parse("8.1851780346865681E+307"), JToken.Parse("1.0066367885961673E+308"))
            // >> True
            private readonly HashSet<JToken> jTokenSet = new HashSet<JToken>(JsonTokenEqualityComparer.Value);

            public bool Add(JToken jToken, out UInt192? hash)
            {
                hash = null;
                return this.jTokenSet.Add(jToken);
            }
        }

        private static string GetRandomName(Random rand)
        {
            StringBuilder stringBuilder = new StringBuilder();
            for (int i = 0; i < rand.Next(0, 100); i++)
            {
                stringBuilder.Append('a' + rand.Next(0, 26));
            }

            return stringBuilder.ToString();
        }

        private static City GetRandomCity(Random rand)
        {
            int index = rand.Next(0, 3);
            switch (index)
            {
                case 0:
                    return City.LosAngeles;
                case 1:
                    return City.NewYork;
                case 2:
                    return City.Seattle;
            }

            return City.LosAngeles;
        }

        private static double GetRandomIncome(Random rand)
        {
            return rand.NextDouble() * double.MaxValue;
        }

        private static int GetRandomAge(Random rand)
        {
            return rand.Next();
        }

        private static Pet GetRandomPet(Random rand)
        {
            string name = CrossPartitionQueryTests.GetRandomName(rand);
            int age = CrossPartitionQueryTests.GetRandomAge(rand);
            return new Pet(name, age);
        }

        public static Person GetRandomPerson(Random rand)
        {
            string name = CrossPartitionQueryTests.GetRandomName(rand);
            City city = CrossPartitionQueryTests.GetRandomCity(rand);
            double income = CrossPartitionQueryTests.GetRandomIncome(rand);
            List<Person> people = new List<Person>();
            if (rand.Next(0, 11) % 10 == 0)
            {
                for (int i = 0; i < rand.Next(0, 5); i++)
                {
                    people.Add(CrossPartitionQueryTests.GetRandomPerson(rand));
                }
            }

            Person[] children = people.ToArray();
            int age = CrossPartitionQueryTests.GetRandomAge(rand);
            Pet pet = CrossPartitionQueryTests.GetRandomPet(rand);
            Guid guid = Guid.NewGuid();
            return new Person(name, city, income, children, age, pet, guid);
        }

        public sealed class JsonTokenEqualityComparer : IEqualityComparer<JToken>
        {
            public static JsonTokenEqualityComparer Value = new JsonTokenEqualityComparer();

            public bool Equals(double double1, double double2)
            {
                return double1 == double2;
            }

            public bool Equals(string string1, string string2)
            {
                return string1.Equals(string2);
            }

            public bool Equals(bool bool1, bool bool2)
            {
                return bool1 == bool2;
            }

            public bool Equals(JArray jArray1, JArray jArray2)
            {
                if (jArray1.Count != jArray2.Count)
                {
                    return false;
                }

                IEnumerable<Tuple<JToken, JToken>> pairwiseElements = jArray1
                    .Zip(jArray2, (first, second) => new Tuple<JToken, JToken>(first, second));
                bool deepEquals = true;
                foreach (Tuple<JToken, JToken> pairwiseElement in pairwiseElements)
                {
                    deepEquals &= this.Equals(pairwiseElement.Item1, pairwiseElement.Item2);
                }

                return deepEquals;
            }

            public bool Equals(JObject jObject1, JObject jObject2)
            {
                if (jObject1.Count != jObject2.Count)
                {
                    return false;
                }

                bool deepEquals = true;
                foreach (KeyValuePair<string, JToken> kvp in jObject1)
                {
                    string name = kvp.Key;
                    JToken value1 = kvp.Value;

                    if (jObject2.TryGetValue(name, out JToken value2))
                    {
                        deepEquals &= this.Equals(value1, value2);
                    }
                    else
                    {
                        return false;
                    }
                }

                return deepEquals;
            }

            public bool Equals(JToken jToken1, JToken jToken2)
            {
                if (Object.ReferenceEquals(jToken1, jToken2))
                {
                    return true;
                }

                if (jToken1 == null || jToken2 == null)
                {
                    return false;
                }

                JsonType type1 = JTokenTypeToJsonType(jToken1.Type);
                JsonType type2 = JTokenTypeToJsonType(jToken2.Type);

                // If the types don't match
                if (type1 != type2)
                {
                    return false;
                }

                switch (type1)
                {

                    case JsonType.Object:
                        return this.Equals((JObject)jToken1, (JObject)jToken2);
                    case JsonType.Array:
                        return this.Equals((JArray)jToken1, (JArray)jToken2);
                    case JsonType.Number:
                        return this.Equals((double)jToken1, (double)jToken2);
                    case JsonType.String:
                        return this.Equals(jToken1.ToString(), jToken2.ToString());
                    case JsonType.Boolean:
                        return this.Equals((bool)jToken1, (bool)jToken2);
                    case JsonType.Null:
                        return true;
                    default:
                        throw new ArgumentException();
                }
            }

            public int GetHashCode(JToken obj)
            {
                return 0;
            }

            private enum JsonType
            {
                Number,
                String,
                Null,
                Array,
                Object,
                Boolean
            }

            private static JsonType JTokenTypeToJsonType(JTokenType type)
            {
                switch (type)
                {

                    case JTokenType.Object:
                        return JsonType.Object;
                    case JTokenType.Array:
                        return JsonType.Array;
                    case JTokenType.Integer:
                    case JTokenType.Float:
                        return JsonType.Number;
                    case JTokenType.Guid:
                    case JTokenType.Uri:
                    case JTokenType.TimeSpan:
                    case JTokenType.Date:
                    case JTokenType.String:
                        return JsonType.String;
                    case JTokenType.Boolean:
                        return JsonType.Boolean;
                    case JTokenType.Null:
                        return JsonType.Null;
                    case JTokenType.None:
                    case JTokenType.Undefined:
                    case JTokenType.Constructor:
                    case JTokenType.Property:
                    case JTokenType.Comment:
                    case JTokenType.Raw:
                    case JTokenType.Bytes:
                    default:
                        throw new ArgumentException();
                }
            }
        }

        public enum City
        {
            NewYork,
            LosAngeles,
            Seattle
        }
        public sealed class Pet
        {
            [JsonProperty("name")]
            public string Name { get; }

            [JsonProperty("age")]
            public int Age { get; }

            public Pet(string name, int age)
            {
                this.Name = name;
                this.Age = age;
            }
        }

        public sealed class Person
        {
            [JsonProperty("name")]
            public string Name { get; }

            [JsonProperty("city")]
            [JsonConverter(typeof(StringEnumConverter))]
            public City City { get; }

            [JsonProperty("income")]
            public double Income { get; }

            [JsonProperty("children")]
            public Person[] Children { get; }

            [JsonProperty("age")]
            public int Age { get; }

            [JsonProperty("pet")]
            public Pet Pet { get; }

            [JsonProperty("guid")]
            public Guid Guid { get; }

            public Person(string name, City city, double income, Person[] children, int age, Pet pet, Guid guid)
            {
                this.Name = name;
                this.City = city;
                this.Income = income;
                this.Children = children;
                this.Age = age;
                this.Pet = pet;
                this.Guid = guid;
            }
        }
    }
}