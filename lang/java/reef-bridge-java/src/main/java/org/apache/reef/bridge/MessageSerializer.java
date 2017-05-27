/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.reef.bridge;

import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificRecord;
import java.io.IOException;
import java.io.ByteArrayOutputStream;

/**
 * Base interface for anonymous message serializer objects.
 */
interface MessageSerializer {
  void serialize(ByteArrayOutputStream outputStream, SpecificRecord message) throws IOException;
}

/**
 * Abstract implementation of message serializer that carries the message type.
 * @param <TMessage> The type of message the instantiation can serialize.
 */
abstract class GenericMessageSerializer<TMessage> implements MessageSerializer {
  Class<TMessage> msgMetaClass;
  public GenericMessageSerializer(final Class<TMessage> msgMetaClass) {
    this.msgMetaClass = msgMetaClass;
  }
  abstract public void serialize(ByteArrayOutputStream outputStream, SpecificRecord message) throws IOException;
}

/**
 * Base interface for ananymous message deserializer objects.
 */
interface MessageDeserializer {
  void deserialize(BinaryDecoder decoder, MultiObserver observer) throws Exception;
}

/**
 * Abstract implementation of message deserializer that carries the message type.
 * @param <TMessage> The type of message the instantiation can deserialize.
 */
abstract class GenericMessageDeserializer<TMessage> implements  MessageDeserializer {
  Class<TMessage> msgMetaClass;
  public GenericMessageDeserializer(final Class<TMessage> msgMetaClass) {
    this.msgMetaClass = msgMetaClass;
  }
  abstract public void deserialize(BinaryDecoder decoder, MultiObserver observer) throws Exception;
}
