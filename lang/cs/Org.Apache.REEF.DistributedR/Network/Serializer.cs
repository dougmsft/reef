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
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Runtime.InteropServices;
using Org.Apache.REEF.Utilities.Logging;
using Microsoft.Hadoop.Avro;

namespace Org.Apache.REEF.DistributedR.Network
{
    /// <summary>
    /// Serializes and desearializes distributed R messages with a header
    /// that identifies the message type. All messages types are automatically
    /// registered via type reflection.
    /// </summary>
    public static class Serializer
    {
        private static readonly Logger Logr = Logger.GetLogger(typeof(TaskRunner));

        private delegate void Serialize(MemoryStream stream, IMessage message);
        private delegate void Deserialize(MemoryStream stream, object observer);

        private static SortedDictionary<Guid, Serialize> serializeMap = new SortedDictionary<Guid, Serialize>();
        private static SortedDictionary<Guid, Deserialize> deserializeMap = new SortedDictionary<Guid, Deserialize>();

        /// <summary>
        /// Use reflection to find and register all messages derived from the client class.
        /// </summary>
        static Serializer()
        {
            Assembly assembly = typeof(Serializer).Assembly;
            Logr.Log(Level.Info, string.Format("Retrieving types for assembly: {0}", assembly.FullName));
            Type[] types = assembly.GetTypes();
            foreach (Type type in types)
            {
                if (type.FullName.ToLower().Contains("distributedr.network"))
                {
                    Type baseType = type.BaseType;
                    while (baseType != null)
                    {
                        if (baseType.Name.ToLower().Contains("serializerclient"))
                        {
                            Logr.Log(Level.Info, string.Format("Found type: {0}", type.FullName));
                            MethodInfo methodInfo = baseType.GetMethod(
                                "Initialize", BindingFlags.IgnoreCase | BindingFlags.Static | BindingFlags.NonPublic);
                            if (methodInfo != null)
                            {
                                methodInfo.Invoke(null, null);
                            }
                        }
                        baseType = baseType.BaseType;
                    }
                }
            }
        }

        /// <summary>
        /// Generates and stores the metadata necessary to serialze
        /// deserialize a specific message type.
        /// </summary>
        /// <typeparam name="TMessage"></typeparam>
        internal static void Register<TMessage>()
        {
            Type type = typeof(TMessage);
            Guid guid = GetGuid(type);

            Serialize serialize = (MemoryStream stream, IMessage message) =>
            {
                IAvroSerializer<TMessage> messageWriter = AvroSerializer.Create<TMessage>();
                messageWriter.Serialize(stream, (TMessage)message);
            };
            serializeMap.Add(guid, serialize);

            Deserialize deserialize = (MemoryStream stream, object observer) =>
            {
                IAvroSerializer<TMessage> messageReader = AvroSerializer.Create<TMessage>();
                TMessage message = messageReader.Deserialize(stream);
                IObserver<TMessage> msgObserver = observer as IObserver<TMessage>;
                if (msgObserver != null)
                {
                    msgObserver.OnNext(message);
                }
                else
                {
                    Logr.Log(Level.Info, string.Format("Unhandled message received: " + message.ToString()));
                }
            };
            deserializeMap.Add(guid, deserialize);
        }

        /// <summary>
        /// Retrieves the GUID attribute for the specified type.
        /// </summary>
        /// <param name="type">An instance of System.Type.</param>
        /// <returns>Guid</returns>
        private static Guid GetGuid(Type type)
        {
            GuidAttribute attribute = (GuidAttribute)Attribute.GetCustomAttribute(type, typeof(GuidAttribute));
            if (attribute != null)
            {
                return new Guid(attribute.Value);
            }
            else
            {
                throw new Exception("Distributed R messages must have a Guid attribute");
            }
        }

        /// <summary>
        /// Serialize the input message and return a byte array.
        /// </summary>
        /// <param name="message">A messeage to be serialized which derives from Client<TMessage></TMessage></param>
        /// <returns>A byte array containing the serialized message and header.</returns>
        public static byte[] Write(IMessage message) 
        {
            try
            { 
                IAvroSerializer<Header> headWriter = AvroSerializer.Create<Header>();
                using (MemoryStream stream = new MemoryStream())
                {
                    Guid id = GetGuid(message.GetType());
                    Header header;
                    header.Id = id;
                    headWriter.Serialize(stream, header);

                    Serialize serialize;
                    if (serializeMap.TryGetValue(id, out serialize))
                    {
                        serialize(stream, message);
                    }
                    else
                    {
                        throw new Exception("Request to serialize unknown message type.");
                    }
                    return stream.GetBuffer();
                }
            }
            catch (Exception e)
            {
                Logr.Log(Level.Error, "Failure writing message: " + e.Message);
                throw e;
            }
        }

        /// <summary>
        /// Read a message from the input byte array.
        /// </summary>
        /// <typeparam name="TReceiver">The concrete type of the object which implements the IReceiver interface.</typeparam>
        /// <param name="data">The byte array containing the message to be deserialized.</param>
        /// <param name="observer">An instance of the TReceiver object whose methods will be invoked base on the message type.</param>
        public static void Read(byte[] data, object observer)
        {
            try
            {
                IAvroSerializer<Header> headReader = AvroSerializer.Create<Header>();
                using (MemoryStream stream = new MemoryStream(data))
                {
                    Header head = headReader.Deserialize(stream);
                    Deserialize deserialize;
                    if (deserializeMap.TryGetValue(head.Id, out deserialize))
                    {
                        deserialize(stream, observer);
                    }
                    else
                    {
                        throw new Exception("Request to deserialize unknown message type.");
                    }
                }
            }
            catch (Exception e)
            {
                Logr.Log(Level.Error, "Failure reading message: " + e.Message);
                throw e;
            }
        }
    }
}
