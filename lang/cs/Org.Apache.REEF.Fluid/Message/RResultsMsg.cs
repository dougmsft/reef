// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//<auto-generated />
namespace Org.Apache.REEF.Fluid.Message
{
    using System;
    using System.Collections.Generic;
    using System.Runtime.Serialization;
    using Microsoft.Hadoop.Avro;

    /// <summary>
    /// Used to serialize and deserialize Avro record Org.Apache.REEF.Fluid.Message.RResultsMsg.
    /// </summary>
    [DataContract(Namespace = "Org.Apache.REEF.Fluid.Message")]
    public partial class RResultsMsg
    {
        private const string JsonSchema = @"{""type"":""record"",""name"":""Org.Apache.REEF.Fluid.Message.RResultsMsg"",""doc"":""Results from the execution an R function on a REEF evaluator."",""fields"":[{""name"":""uuid"",""doc"":""Universially unique indentifier of the task."",""type"":""string""},{""name"":""value"",""doc"":""Results generated from the execution of an R function on a REEF evaluator."",""type"":""string""}]}";

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
        /// Gets or sets the uuid field.
        /// </summary>
        [DataMember]
        public string uuid { get; set; }
              
        /// <summary>
        /// Gets or sets the value field.
        /// </summary>
        [DataMember]
        public string value { get; set; }
                
        /// <summary>
        /// Initializes a new instance of the <see cref="RResultsMsg"/> class.
        /// </summary>
        public RResultsMsg()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="RResultsMsg"/> class.
        /// </summary>
        /// <param name="uuid">The uuid.</param>
        /// <param name="value">The value.</param>
        public RResultsMsg(string uuid, string value)
        {
            this.uuid = uuid;
            this.value = value;
        }
    }
}
