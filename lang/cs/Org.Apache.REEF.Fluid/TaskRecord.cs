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

using System;

namespace Org.Apache.REEF.Fluid
{
    internal class TaskRecord
    {
        public Guid UUID { get; set; }
        public string Function { get; set; }
        public string Data { get; set; }

        public TaskRecord(Guid uuid, string function, string data)
        {
            UUID = uuid;
            Function = function;
            Data = data;
        }
    }

    internal class RTaskRecord : TaskRecord
    {
        public RTaskRecord(Guid uuid, string function, string data) : base(uuid, function, data)
        {
        }
    }

    internal class JuliaTaskRecord : TaskRecord
    {
        public JuliaTaskRecord(Guid uuid, string function, string data) : base(uuid, function, data)
        {
        }
    }
}
