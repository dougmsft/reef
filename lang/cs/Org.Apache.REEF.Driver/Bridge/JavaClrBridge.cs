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

using System.IO;
using Org.Apache.REEF.Common.Files;
using Org.Apache.REEF.IO.FileSystem;
using Org.Apache.REEF.IO.FileSystem.Local;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Utilities.Logging;
  
namespace Org.Apache.REEF.Driver.Bridge
{
    class JavaClrBridge
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(JavaClrBridge));

        public JavaClrBridge()
        {
            // Instantiate a file system proxy.
            IFileSystem fileSystem = TangFactory.GetTang()
                .NewInjector(LocalFileSystemConfiguration.ConfigurationModule.Build())
                .GetInstance<IFileSystem>();

            // Get the path to the bridge name server endpoint file.
            REEFFileNames fileNames = new REEFFileNames();

            string nameServerAddress = null;
            using (FileStream stream = File.Open(fileNames.GetDriverNameServerEndpoint(), FileMode.Open))
            {
                using (StreamReader reader = new StreamReader(stream))
                {
                    nameServerAddress = reader.ReadToEnd();
                }
            }
            Logger.Log(Level.Info, string.Format("Name Server Address: {0}", (nameServerAddress == null) ? "NULL" : nameServerAddress));
        }
    }
}
