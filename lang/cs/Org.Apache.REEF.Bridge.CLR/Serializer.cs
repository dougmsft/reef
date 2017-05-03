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
using Org.Apache.REEF.Utilities.Logging;
using org.apache.reef.bridge.message;
using Microsoft.Hadoop.Avro;

namespace Org.Apache.REEF.Bridge
{
    /// <summary>    /// Serializes and desearializes distributed R messages with a header    /// that identifies the message type. All messages types are automatically    /// registered via type reflection at startup.    /// </summary>
    public static class Serializer
    {
        private static readonly Logger Logr = Logger.GetLogger(typeof(Serializer));

        private delegate void Serialize(MemoryStream stream, object message);        private delegate void Deserialize(MemoryStream stream, object observer);        private static SortedDictionary<Guid, int> guidToIntMap = new SortedDictionary<Guid, int>();        private static SortedDictionary<int, Serialize> serializeMap = new SortedDictionary<int, Serialize>();        private static SortedDictionary<int, Deserialize> deserializeMap = new SortedDictionary<int, Deserialize>();        public static Dictionary<string, int> classMap = new Dictionary<string, int>();

        /// <summary>        /// Use reflection to find and register all messages.        /// </summary>
        public static void Initialize()
        {
            MethodInfo registerInfo = typeof(Serializer).GetMethod("Register", BindingFlags.Static | BindingFlags.NonPublic);            MethodInfo genericInfo;            Assembly assembly = typeof(Serializer).Assembly;            Logr.Log(Level.Info, string.Format("Retrieving types for assembly: {0}", assembly.FullName));            Type[] types = assembly.GetTypes();            int index = 101;            foreach (Type type in types)            {                string name = type.FullName;                if (name.Contains("org.apache.reef.bridge.message"))                {                    classMap[name] = index++;                    genericInfo = registerInfo.MakeGenericMethod(new[] { type });                    if (name.Contains("Protocol"))
                    {
                        // Protocol messages always have index 100.
                        genericInfo.Invoke(null, new object[] { 100 });                    }                    else
                    {
                        genericInfo.Invoke(null, new object[] { index++ });
                    }                }            }
        }

        /// <summary>        /// Generates and stores the metadata necessary to serialze and deserialize a specific message type.        /// </summary>        /// <typeparam name="TMessage">The class type of the message being registered.</typeparam>        /// <param name="identifier">An integer whose value in the header message desigates the message type being registered.</param>        internal static void Register<TMessage>(int identifier)        {            Logr.Log(Level.Info, string.Format("Registering message type: {0}", typeof(TMessage).FullName));

            guidToIntMap.Add(typeof(TMessage).GUID, identifier);
            Serialize serialize = (MemoryStream stream, object message) =>            {                IAvroSerializer<TMessage> messageWriter = AvroSerializer.Create<TMessage>();                messageWriter.Serialize(stream, (TMessage)message);            };            serializeMap.Add(identifier, serialize);
            Deserialize deserialize = (MemoryStream stream, object observer) =>            {                IAvroSerializer<TMessage> messageReader = AvroSerializer.Create<TMessage>();                TMessage message = messageReader.Deserialize(stream);                IObserver<TMessage> msgObserver = observer as IObserver<TMessage>;                if (msgObserver != null)                {                    msgObserver.OnNext(message);                }                else                {                    Logr.Log(Level.Info, string.Format("Unhandled message received: " + message.ToString()));                }            };            deserializeMap.Add(identifier, deserialize);        }
        /// <summary>        /// Serialize the input message and return a byte array.        /// </summary>        /// <param name="message">A object reference to a messeage to be serialized</param>        /// <returns>A byte array containing the serialized associated header and message.</returns>        public static byte[] Write(object message)         {            try            {                 IAvroSerializer<Header> headWriter = AvroSerializer.Create<Header>();                using (MemoryStream stream = new MemoryStream())                {                    int identifier = guidToIntMap[message.GetType().GUID];                    Header header = new Header(identifier);                    headWriter.Serialize(stream, header);
                    Serialize serialize;                    if (serializeMap.TryGetValue(identifier, out serialize))                    {                        serialize(stream, message);                    }                    else                    {                        throw new Exception("Request to serialize unknown message type.");                    }                    return stream.GetBuffer();                }            }            catch (Exception e)            {                Logr.Log(Level.Error, "Failure writing message: " + e.Message);                throw e;            }        }
        /// <summary>        /// Read a message from the input byte array.        /// </summary>        /// <param name="data">The byte array containing header message and message to be deserialized.</param>        /// <param name="observer">An object which implements the IObserver<> interface for the message being deserialized.</param>        public static void Read(byte[] data, object observer)        {            try            {                IAvroSerializer<Header> headReader = AvroSerializer.Create<Header>();                using (MemoryStream stream = new MemoryStream(data))                {                    Header head = headReader.Deserialize(stream);                    Deserialize deserialize;                    if (deserializeMap.TryGetValue(head.identifier, out deserialize))                    {                        deserialize(stream, observer);                    }                    else                    {                        throw new Exception("Request to deserialize unknown message type.");                    }                }            }            catch (Exception e)            {                Logr.Log(Level.Error, "Failure reading message: " + e.Message);                throw e;            }        }
    }
}
