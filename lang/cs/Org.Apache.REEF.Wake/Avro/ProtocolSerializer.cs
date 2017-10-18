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
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using Microsoft.Hadoop.Avro;
using Org.Apache.REEF.Utilities.Logging;
using org.apache.reef.wake.avro.message;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Wake.Avro
{
    /// <summary>
    /// Given a namespace of Avro messages which represent a protocol, the ProtocolSerailizer
    /// class reflects all of the message classes and builds Avro seriailziers and deserializers.
    /// Avro messages are then serialized/deserilaized to and from byte[] arrays using the
    /// Read/Write methods. A transport such as a RemoteObserver using a ByteCodec can then
    /// be used to send and receive the serialized messages.
    /// </summary>
    public sealed class ProtocolSerializer
    {
        [NamedParameter("Name of the assembly that contains serializable classes.")]
        public class AssemblyName : Name<string>
        {
        }

        [NamedParameter("Package name to search for serializabe classes.")]
        public class MessageNamespace : Name<string>
        {
        }

        private static readonly Logger Logr = Logger.GetLogger(typeof(ProtocolSerializer));

        /// <summary>
        /// Delegate for message serializer.
        /// </summary>
        private delegate void Serialize(MemoryStream stream, object message);

        /// <summary>
        /// Delegate for message deserializer.
        /// </summary>
        /// <param name="observer">Must be of type IObserver&lt;IMessageInstance&lt;?&gt;</param>
        private delegate void Deserialize(MemoryStream stream, object observer, long sequence);

        /// <summary>
        /// Map from message type (a string with the message class name) to serializer.
        /// </summary>
        private readonly SortedDictionary<string, Serialize>
            serializeMap = new SortedDictionary<string, Serialize>();

        /// <summary>
        /// Map from message type (a string with the message class name) to deserializer.
        /// </summary>
        private readonly SortedDictionary<string, Deserialize>
            deserializeMap = new SortedDictionary<string, Deserialize>();

        private readonly IAvroSerializer<Header> headerSerializer = AvroSerializer.Create<Header>();

        /// <summary>
        /// Non-generic reflection record for the Register() method of this class. A constant.
        /// </summary>
        private static readonly MethodInfo RegisterMethodInfo =
            typeof(ProtocolSerializer).GetMethod("Register", BindingFlags.Instance | BindingFlags.NonPublic);

        /// <summary>
        /// Construct an initialized protocol serializer.
        /// </summary>
        /// <param name="assemblyName">The full name of the assembly which contains the namespace of the message classes.</param>
        /// <param name="messageNamespace">A string which contains the namespace the protocol messages.</param>
        [Inject]
        public ProtocolSerializer(
            [Parameter(typeof(AssemblyName))] string assemblyName,
            [Parameter(typeof(MessageNamespace))] string messageNamespace)
        {
            Logr.Log(Level.Verbose, "Retrieving types for assembly: {0}", assemblyName);
            Assembly assembly = Assembly.Load(assemblyName);

            var types = new List<Type>(assembly.GetTypes()) { typeof(Header) };
            foreach (Type type in types)
            {
                if (type.FullName.StartsWith(messageNamespace))
                {
                    MethodInfo genericInfo = RegisterMethodInfo.MakeGenericMethod(new[] { type });
                    genericInfo.Invoke(this, null);
                }
            }
        }

        /// <summary>
        /// Generate and store the metadata necessary to serialze and deserialize a specific message type.
        /// </summary>
        /// <typeparam name="TMessage">The class type of the message being registered.</typeparam>
        internal void Register<TMessage>()
        {
            Logr.Log(Level.Info, "Registering message type: {0} {1}",
                typeof(TMessage).FullName, typeof(TMessage).Name);

            IAvroSerializer<TMessage> messageSerializer = AvroSerializer.Create<TMessage>();
            Serialize serialize = (MemoryStream stream, object message) =>
            {
                messageSerializer.Serialize(stream, (TMessage)message);
            };
            serializeMap.Add(typeof(TMessage).FullName, serialize);

            Deserialize deserialize = (MemoryStream stream, object observer, long sequence) =>
            {
                TMessage message = messageSerializer.Deserialize(stream);
                var msgObserver = observer as IObserver<IMessageInstance<TMessage>>;
                if (msgObserver != null)
                {
                    Logr.Log(Level.Info, "Invoking message observer {0} with message {1}", msgObserver, message);
                    msgObserver.OnNext(new MessageInstance<TMessage>(sequence, message));
                }
                else
                {
                    Logr.Log(Level.Warning, "Unhandled message received: {0}", message);
                }
            };
            deserializeMap.Add(typeof(TMessage).FullName, deserialize);
        }

        /// <summary>
        /// Serialize the input message and return a byte array.
        /// </summary>
        /// <param name="message">An object reference to the messeage to be serialized</param>
        /// <param name="sequence">
        /// A long which cotains the higher level protocols sequence number for the message.</param>
        /// <returns>A byte array containing the serialized header and message.</returns>
        public byte[] Write(object message, long sequence) 
        {
            string name = message.GetType().FullName;
            Logr.Log(Level.Info, "Serializing message: {0}", name);
            try
            { 
                using (MemoryStream stream = new MemoryStream())
                {
                    Header header = new Header(sequence, name);
                    headerSerializer.Serialize(stream, header);

                    Serialize serialize;
                    if (serializeMap.TryGetValue(name, out serialize))
                    {
                        serialize(stream, message);
                    }
                    else
                    {
                        throw new SeializationException(
                            "Request to serialize unknown message type: " + name);
                    }
                    return stream.GetBuffer();
                }
            }
            catch (Exception e)
            {
                Logr.Log(Level.Error, "Failure writing message.", e);
                throw e;
            }
        }

        /// <summary>
        /// Read a message from the input byte array.
        /// </summary>
        /// <param name="data">Byte array containing a header message and message to be deserialized.</param>
        /// <param name="observer">An object which implements the IObserver<>
        /// interface for the message being deserialized.</param>
        public void Read(byte[] data, object observer)
        {
            try
            {
                using (MemoryStream stream = new MemoryStream(data))
                {
                    Header head = headerSerializer.Deserialize(stream);
                    Logr.Log(Level.Info, "Message header {0}", head);
                    Deserialize deserialize;
                    if (deserializeMap.TryGetValue(head.className, out deserialize))
                    {
                        deserialize(stream, observer, head.sequence);
                    }
                    else
                    {
                        throw new SeializationException(
                            "Request to deserialize unknown message type: " + head.className);
                    }
                }
            }
            catch (Exception e)
            {
                Logr.Log(Level.Error, "Failure reading message.", e);
                throw e;
            }
        }
    }
}
