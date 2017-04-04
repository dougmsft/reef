﻿// Licensed to the Apache Software Foundation (ASF) under one
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

using System;
using System.Runtime.InteropServices;
using System.Runtime.Serialization;

namespace Org.Apache.REEF.DistributedR.Network
{
    [DataContract(Name = "RTaskMsg")]
    [Guid("D317BAB5-A56C-4C0A-A931-CD3490947AFA")]
    public sealed class RTaskMsg : SerializerClient<RTaskMsg>
    {
        [DataMember]
        public Guid Id { get; set; }

        [DataMember]
        public string Function { get; set; }

        [DataMember]
        public string Data { get; set; }

        public static RTaskMsg Factory(string function, string data)
        {
            RTaskMsg rtask = new RTaskMsg();
            rtask.Id = Guid.NewGuid();
            rtask.Function = function;
            rtask.Data = data;
            return rtask;
        }

        public override string ToString()
        {
            return string.Format("RTaskMsg[{0}] function = [{1}] data = [{2}]", Id.ToString(), Function, Data);
        }
    }
}
