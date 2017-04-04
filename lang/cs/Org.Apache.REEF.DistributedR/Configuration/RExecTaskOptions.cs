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

using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.DistributedR
{
    public sealed class RExecTaskOptions
    {
        [NamedParameter(DefaultValue = "cat('NO RSCRIPT')", Documentation = "The default R Script to execute")]
        public class Function : Name<string>
        {
        }

        [NamedParameter(DefaultValue = "THE DATA", Documentation = "The default data parameter")]
        public class Data : Name<string>
        {
        }
    }

    public sealed class RExecTaskConfiguration : ConfigurationModuleBuilder
    {
        public static readonly RequiredParameter<string> Function = new RequiredParameter<string>();
        public static readonly RequiredParameter<string> Data = new RequiredParameter<string>();

        public static readonly ConfigurationModule ConfigurationModule = new RExecTaskConfiguration()
            .BindNamedParameter(GenericType<RExecTaskOptions.Function>.Class, Function)
            .BindNamedParameter(GenericType<RExecTaskOptions.Data>.Class, Data)
            .Build();
    }
}
