﻿//-----------------------------------------------------------------------
// <copyright file="CosmosElement.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace Microsoft.Azure.Cosmos.CosmosElements
{
    using System;
    using Microsoft.Azure.Cosmos.Json;
    using System.Text;

    [Newtonsoft.Json.JsonConverter(typeof(CosmosElementJsonConverter))]
    internal abstract class CosmosElement
    {
        protected CosmosElement(CosmosElementType cosmosItemType)
        {
            this.Type = cosmosItemType;
        }

        public CosmosElementType Type
        {
            get;
        }

        public override string ToString()
        {
            IJsonWriter jsonWriter = JsonWriter.Create(JsonSerializationFormat.Text);
            this.WriteTo(jsonWriter);
            return Encoding.UTF8.GetString(jsonWriter.GetResult());
        }

        public abstract void WriteTo(IJsonWriter jsonWriter);

        public static CosmosElement Create(byte[] buffer)
        {
            IJsonNavigator jsonNavigator = JsonNavigator.Create(buffer);
            IJsonNavigatorNode jsonNavigatorNode = jsonNavigator.GetRootNode();

            return CosmosElement.Dispatch(jsonNavigator, jsonNavigatorNode);
        }

        public static CosmosElement Dispatch(
            IJsonNavigator jsonNavigator,
            IJsonNavigatorNode jsonNavigatorNode)
        {
            JsonNodeType jsonNodeType = jsonNavigator.GetNodeType(jsonNavigatorNode);
            CosmosElement item;
            switch (jsonNodeType)
            {
                case JsonNodeType.Null:
                    item = CosmosNull.Create();
                    break;

                case JsonNodeType.False:
                    item = CosmosBoolean.Create(false);
                    break;

                case JsonNodeType.True:
                    item = CosmosBoolean.Create(true);
                    break;

                case JsonNodeType.Number:
                    item = CosmosNumber64.Create(jsonNavigator, jsonNavigatorNode);
                    break;

                case JsonNodeType.FieldName:
                case JsonNodeType.String:
                    item = CosmosString.Create(jsonNavigator, jsonNavigatorNode);
                    break;

                case JsonNodeType.Array:
                    item = CosmosArray.Create(jsonNavigator, jsonNavigatorNode);
                    break;

                case JsonNodeType.Object:
                    item = CosmosObject.Create(jsonNavigator, jsonNavigatorNode);
                    break;

                case JsonNodeType.Int8:
                    item = CosmosInt8.Create(jsonNavigator, jsonNavigatorNode);
                    break;

                case JsonNodeType.Int16:
                    item = CosmosInt16.Create(jsonNavigator, jsonNavigatorNode);
                    break;

                case JsonNodeType.Int32:
                    item = CosmosInt32.Create(jsonNavigator, jsonNavigatorNode);
                    break;

                case JsonNodeType.Int64:
                    item = CosmosInt64.Create(jsonNavigator, jsonNavigatorNode);
                    break;

                case JsonNodeType.UInt32:
                    item = CosmosUInt32.Create(jsonNavigator, jsonNavigatorNode);
                    break;

                case JsonNodeType.Float32:
                    item = CosmosFloat32.Create(jsonNavigator, jsonNavigatorNode);
                    break;

                case JsonNodeType.Float64:
                    item = CosmosFloat64.Create(jsonNavigator, jsonNavigatorNode);
                    break;

                case JsonNodeType.Guid:
                    item = CosmosGuid.Create(jsonNavigator, jsonNavigatorNode);
                    break;

                case JsonNodeType.Binary:
                    item = CosmosBinary.Create(jsonNavigator, jsonNavigatorNode);
                    break;

                default:
                    throw new ArgumentException($"Unknown {nameof(JsonNodeType)}: {jsonNodeType}");
            }

            return item;
        }
    }
}
