//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos
{
    using System;
    using System.Globalization;
    using Microsoft.Azure.Documents;

    /// <summary>
    /// The Cosmos Change Feed request options
    /// </summary>
    internal class CosmosChangeFeedRequestOptions : CosmosRequestOptions
    {
        private const string IfNoneMatchAllHeaderValue = "*";

        /// <summary>
        /// Maximum response size for the feed read measured in items.
        /// </summary>
        public virtual int? MaxItemCount { get; set; }

        /// <summary>
        /// Continuation from a previous request.
        /// </summary>
        public virtual string RequestContinuation { get; set; }

        /// <summary>
        /// Marks whether the change feed should be read from the start.
        /// </summary>
        /// <remarks>
        /// If this is specified, StartTime is ignored.
        /// </remarks>
        public virtual bool StartFromBeginning { get; set; }

        /// <summary>
        /// Specifies a particular point in time to start to read the change feed.
        /// </summary>
        public virtual DateTime? StartTime { get; set; }

        internal virtual string PartitionKeyRangeId { get; set; }

        /// <summary>
        /// Fill the CosmosRequestMessage headers with the set properties
        /// </summary>
        /// <param name="request">The <see cref="CosmosRequestMessage"/></param>
        public override void FillRequestOptions(CosmosRequestMessage request)
        {
            if (!string.IsNullOrWhiteSpace(this.RequestContinuation))
            {
                // On REST level, change feed is using IfNoneMatch/ETag instead of continuation
                request.Headers.IfNoneMatch = this.RequestContinuation;
            }

            if (this.MaxItemCount != null && this.MaxItemCount.HasValue)
            {
                request.Headers.Add(HttpConstants.HttpHeaders.PageSize, this.MaxItemCount.Value.ToString(CultureInfo.InvariantCulture));
            }

            if (string.IsNullOrEmpty(request.Headers.IfNoneMatch))
            {
                if (!this.StartFromBeginning && this.StartTime == null)
                {
                    request.Headers.IfNoneMatch = CosmosChangeFeedRequestOptions.IfNoneMatchAllHeaderValue;
                }
                else if (this.StartTime != null)
                {
                    request.Headers.Add(HttpConstants.HttpHeaders.IfModifiedSince, this.StartTime.Value.ToUniversalTime().ToString("r", CultureInfo.InvariantCulture));
                }
            }

            request.Headers.Add(HttpConstants.HttpHeaders.A_IM, HttpConstants.A_IMHeaderValues.IncrementalFeed);

            if (!string.IsNullOrEmpty(this.PartitionKeyRangeId))
            {
                request.PartitionKeyRangeId = this.PartitionKeyRangeId;
            }

            base.FillRequestOptions(request);
        }
    }
}