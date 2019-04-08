//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.Query
{
    using System.Collections.Generic;
    using System;
    using System.Linq;
    using Newtonsoft.Json;
    using System.Threading.Tasks;
    using Microsoft.Azure.Cosmos.Routing;
    using System.Diagnostics;

    /// <summary>
    /// Stand by continuation token representing a contiguous read over all the ranges with continuation state across all ranges.
    /// </summary>
    internal class StandByFeedContinuationToken
    {
        private LinkedList<CompositeContinuationToken> compositeContinuationTokens;
        private string inputContinuationToken;
        private string collectionRid { get; }
        private PartitionKeyRangeCache pkRangeCache { get; }

        public StandByFeedContinuationToken(
            string collectionRid,
            string initialStandByFeedContinuationToken,
            PartitionKeyRangeCache pkRangeCache)
        {
            if (string.IsNullOrWhiteSpace(collectionRid)) throw new ArgumentNullException(nameof(collectionRid));
            if (pkRangeCache == null) throw new ArgumentNullException(nameof(pkRangeCache));

            this.pkRangeCache = pkRangeCache;
            this.collectionRid = collectionRid;
            this.inputContinuationToken = initialStandByFeedContinuationToken;
        }

        public async Task<Tuple<CompositeContinuationToken, string>> GetCurrentToken(bool forceRefresh = false)
        {
            await this.EnsureInitialized();
            Debug.Assert(this.compositeContinuationTokens != null);

            CompositeContinuationToken firstToken = this.compositeContinuationTokens.First();
            IReadOnlyList<Documents.PartitionKeyRange> resolvedRanges = await this.TryGetOverlappingRangesAsync(firstToken.Range, forceRefresh: forceRefresh);
            if (resolvedRanges.Count > 1) 
            {
                // Split happened already replace first with the split ranges
                // Don't push it back as it required next partiton visit which also might have been split
                this.compositeContinuationTokens.RemoveFirst();
                foreach(Documents.PartitionKeyRange newRange in resolvedRanges)
                {
                    this.compositeContinuationTokens.AddFirst(new CompositeContinuationToken()
                    {
                        Range = new Documents.Routing.Range<string>(
                            newRange.MinInclusive,
                            newRange.MaxExclusive,
                            isMinInclusive: true,
                            isMaxInclusive: false),
                        Token = firstToken.Token, // Follow token from parent/grand partitions
                    });
                }

                // Reset first token 
                firstToken = this.compositeContinuationTokens.First();
            }

            return new Tuple<CompositeContinuationToken, string>(firstToken, resolvedRanges[0].Id);
        }

        public void MoveToNextToken()
        {
            Debug.Assert(this.compositeContinuationTokens != null);

            CompositeContinuationToken firstToken = this.compositeContinuationTokens.First();
            this.compositeContinuationTokens.RemoveFirst();
            this.compositeContinuationTokens.AddLast(firstToken);
        }

        internal new string ToString()
        {
            if (this.compositeContinuationTokens == null)
            {
                return this.inputContinuationToken;
            }

            return JsonConvert.SerializeObject(this.compositeContinuationTokens.ToList());
        }

        private async Task EnsureInitialized()
        {
            if (this.compositeContinuationTokens == null)
            {
                IEnumerable<CompositeContinuationToken> tokens = await this.BuildCompositeTokens(this.inputContinuationToken);

                this.compositeContinuationTokens = new LinkedList<CompositeContinuationToken>();
                foreach (CompositeContinuationToken token in tokens)
                {
                    this.compositeContinuationTokens.AddLast(token);
                }

                Debug.Assert(this.compositeContinuationTokens.Count > 0);
            }
        }

        private async Task<IEnumerable<CompositeContinuationToken>> BuildCompositeTokens(string initialContinuationToken)
        {
            if (string.IsNullOrEmpty(initialContinuationToken))
            {
                // Initialize composite token with all the ranges
                IReadOnlyList<Documents.PartitionKeyRange> allRanges = await this.pkRangeCache.TryGetOverlappingRangesAsync(
                        this.collectionRid,
                        new Documents.Routing.Range<string>(
                            Documents.Routing.PartitionKeyInternal.MinimumInclusiveEffectivePartitionKey,
                            Documents.Routing.PartitionKeyInternal.MaximumExclusiveEffectivePartitionKey,
                            isMinInclusive: true,
                            isMaxInclusive: false));

                Debug.Assert(allRanges.Count != 0);
                return allRanges.Select(e => new CompositeContinuationToken()
                {
                    Range = new Documents.Routing.Range<string>(e.MinInclusive, e.MaxExclusive, isMinInclusive: true, isMaxInclusive: false),
                    Token = string.Empty,
                });
            }

            try
            {
                return JsonConvert.DeserializeObject<List<CompositeContinuationToken>>(initialContinuationToken);
            }
            catch
            {
                throw new FormatException("Provided token has an invalid format");
            }
        }

        private async Task<IReadOnlyList<Documents.PartitionKeyRange>> TryGetOverlappingRangesAsync(
            Documents.Routing.Range<string> targetRange,
            bool forceRefresh = false)
        {
            if (targetRange == null) throw new ArgumentNullException(nameof(targetRange));

            IReadOnlyList<Documents.PartitionKeyRange> keyRanges = await this.pkRangeCache.TryGetOverlappingRangesAsync(
                    this.collectionRid,
                    new Documents.Routing.Range<string>(
                        targetRange.Min,
                        targetRange.Max,
                        isMaxInclusive:true,
                        isMinInclusive:false),
                    forceRefresh: forceRefresh); ;

            if (keyRanges.Count == 0)
            {
                throw new ArgumentOutOfRangeException("RequestContinuation", $"Token contains invalid range {targetRange.Min}-{targetRange.Max}");
            }

            return keyRanges;
        }
    }
}
