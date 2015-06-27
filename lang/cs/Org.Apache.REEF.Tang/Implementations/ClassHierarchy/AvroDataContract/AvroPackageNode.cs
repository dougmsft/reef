/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//<auto-generated />
namespace Org.Apache.REEF.Tang.Implementations.ClassHierarchy.AvroDataContract
{
    using System;
    using System.Collections.Generic;
    using System.Runtime.Serialization;
    using Microsoft.Hadoop.Avro;

    /// <summary>
    /// Used to serialize and deserialize Avro record org.apache.reef.tang.implementation.avro.AvroPackageNode.
    /// </summary>
    [DataContract(Namespace = "org.apache.reef.tang.implementation.avro")]
    public partial class AvroPackageNode
    {
        private const string JsonSchema = @"{""type"":""record"",""name"":""org.apache.reef.tang.implementation.avro.AvroPackageNode"",""fields"":[]}";

        /// <summary>
        /// Gets the schema.
        /// </summary>
        public static string Schema
        {
            get
            {
                return JsonSchema;
            }
        }
        
        /// <summary>
        /// Initializes a new instance of the <see cref="AvroPackageNode"/> class.
        /// </summary>
        public AvroPackageNode()
        {
        }
    }
}