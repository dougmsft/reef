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

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.reef.bridge.message.Header;
import org.apache.avro.specific.SpecificRecordBase;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import io.github.lukehutch.fastclasspathscanner.FastClasspathScanner;
import io.github.lukehutch.fastclasspathscanner.scanner.ScanResult;

// Base interface for anonymous message serializer objects.
interface MessageSerializer {
  void serialize(ByteArrayOutputStream stream, Object object) throws IOException;
}
// Abstract implementation that carries the message type.
abstract class GenericMessageSerializer<TMessage> implements MessageSerializer {
  Class<TMessage> messageClass;
  public GenericMessageSerializer(Class<TMessage> messageClass) {
    this.messageClass = messageClass;
  }
  abstract public void serialize(ByteArrayOutputStream stream, Object object) throws IOException;
}

/**
 *
 */
final public class Serializer {
  private static final Logger LOG = Logger.getLogger(Serializer.class.getName());
  private static Map<String, MessageSerializer> nameToSerializerMap = new HashMap<>();

  private Serializer() { }

  /**
   *
   */
  public static void Initialize() {
    LOG.log(Level.INFO, "Start: Serializer.Initialize");

    // Build a list of the avro message reflection class.
    ScanResult scanResult = new FastClasspathScanner("org.apache.reef.bridge.message").scan();
    List<String> scanNames = scanResult.getNamesOfSubclassesOf(SpecificRecordBase.class);
    List<Class<?>> messageClasses = scanResult.classNamesToClassRefs(scanNames);

    LOG.log(Level.INFO, "!!!NUMBER OF MESSAGES = " + Integer.toString(messageClasses.size()));
    try {
      // Call register on every message type.
      for (Class<?> cls : messageClasses) {
        LOG.log(Level.INFO, "Found message class: " + cls.getName() + " " + cls.getSimpleName());
        Method register = Serializer.class.getMethod("Register", cls.getClass());
        LOG.log(Level.INFO, "Obtained the method class instance");
        register.invoke(null, cls);
      }
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Failed to register message class " + e.getMessage() );
    }

    LOG.log(Level.INFO, "End: Serializer.Initialize");
  }

  /**
   *
   */
  public static <TMessage> void Register(Class<TMessage> messageClass)
  {
    LOG.log(Level.INFO, "Registering [" + messageClass.getSimpleName() + "]");

    // Instantiate an anonymous instance of the message serializer.
    final MessageSerializer messageSerializer = new GenericMessageSerializer<TMessage>(messageClass) {
      public void serialize(ByteArrayOutputStream stream, Object object) throws IOException {
          // Binary encoder for both the header and message.
          BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(stream, null);

          // Writers for header and message.
          DatumWriter<Header> headerWriter = new SpecificDatumWriter<>(Header.class);
          DatumWriter<TMessage> messageWriter = new SpecificDatumWriter<>(messageClass);

          // Write the header and the message.
          headerWriter.write(new Header(0, messageClass.getSimpleName()), encoder);
          messageWriter.write((TMessage)object, encoder);
      }
    };
    // Add the anonymous function to the name to serializer map.
    nameToSerializerMap.put(messageClass.getSimpleName(), messageSerializer);
  }


}
