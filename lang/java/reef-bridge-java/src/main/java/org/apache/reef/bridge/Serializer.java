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
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.reef.bridge.message.Header;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.reef.wake.rx.Observer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import io.github.lukehutch.fastclasspathscanner.FastClasspathScanner;
import io.github.lukehutch.fastclasspathscanner.scanner.ScanResult;

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
  public GenericMessageSerializer(Class<TMessage> msgMetaClass) {
    this.msgMetaClass = msgMetaClass;
  }
  abstract public void serialize(ByteArrayOutputStream outputStream, SpecificRecord message) throws IOException;
}

/**
 * Base interface for ananymous message deserializer objects.
 */
interface MessageDeserializer {
  void deserialize(BinaryDecoder decoder, Object observer) throws IOException;
}

/**
 * Abstract implementation of message deserializer that carries the message type.
 * @param <TMessage> The type of message the instantiation can deserialize.
 */
abstract class GenericMessageDeserializer<TMessage> implements  MessageDeserializer {
  Class<TMessage> msgMetaClass;
  public GenericMessageDeserializer(Class<TMessage> msgMetaClass) { this.msgMetaClass = msgMetaClass; }
  abstract public void deserialize(BinaryDecoder decoder, Object observer) throws IOException;
}

/**
 *
 */
final public class Serializer {
  private static final Logger LOG = Logger.getLogger(Serializer.class.getName());
  // Maps for mapping message class names to serializer and deserializer classes.
  private static Map<String, MessageSerializer> nameToSerializerMap = new HashMap<>();
  private static Map<String, MessageDeserializer> nameToDeserializerMap = new HashMap<>();

  /**
   *  Class utility with all static methods.
   */
  private Serializer() { }

  /**
   * Finds all of the messages in the org.apache.reef.bridge.message package and calls register.
   */
  public static void Initialize() {
    LOG.log(Level.INFO, "Start: Serializer.Initialize");

    // Build a list of the message reflection classes.
    final ScanResult scanResult = new FastClasspathScanner("org.apache.reef.bridge.message").scan();
    final List<String> scanNames = scanResult.getNamesOfSubclassesOf(SpecificRecordBase.class);
    final List<Class<?>> messageClasses = scanResult.classNamesToClassRefs(scanNames);
    LOG.log(Level.INFO, "!!!NUMBER OF MESSAGES = " + Integer.toString(messageClasses.size()));

    try {
      // Register all of the messages.
      for (Class<?> cls : messageClasses) {
        LOG.log(Level.INFO,"Found message class: " + cls.getName() + " " + cls.getSimpleName());
        Method register = Serializer.class.getMethod("Register", cls.getClass());
        LOG.log(Level.INFO,"Obtained the method class instance");
        register.invoke(null, cls);
      }
    } catch (Exception e) {
      LOG.log(Level.SEVERE,"Failed to register message class " + e.getMessage() );
    }

    LOG.log(Level.INFO,"End: Serializer.Initialize");
  }

  /**
   * Instantiates and adds a message serializer/deserializer for the message.
   * @param msgMetaClass The reflection class for the message.
   * @param <TMessage> The message Java type.
   */
  public static <TMessage> void Register(Class<TMessage> msgMetaClass)
  {
    LOG.log(Level.INFO,"Registering [" + msgMetaClass.getSimpleName() + "]");

    // Instantiate an anonymous instance of the message serializer for this message type.
    final MessageSerializer messageSerializer = new GenericMessageSerializer<TMessage>(msgMetaClass) {

      public void serialize(ByteArrayOutputStream outputStream, SpecificRecord message) throws IOException {
        // Binary encoder for both the header and message.
        final BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);

        // Writers for header and message.
        final DatumWriter<Header> headerWriter = new SpecificDatumWriter<>(Header.class);
        final DatumWriter<TMessage> messageWriter = new SpecificDatumWriter<>(msgMetaClass);

        // Write the header and the message.
        headerWriter.write(new Header(0, msgMetaClass.getSimpleName()), encoder);
        messageWriter.write((TMessage)message, encoder);
        encoder.flush();
      }

    };
    nameToSerializerMap.put(msgMetaClass.getSimpleName(), messageSerializer);

    // Instantiate an anonymous instance of the message deserializer for this message type.
    final MessageDeserializer messageDeserializer = new GenericMessageDeserializer<TMessage>(msgMetaClass) {

      public void deserialize(BinaryDecoder decoder, Object observer) throws IOException {
        final SpecificDatumReader<TMessage> messageReader = new SpecificDatumReader<>(msgMetaClass);
        final TMessage message = messageReader.read(null, decoder);
        if (observer instanceof Observer) {
          ((Observer<TMessage>)observer).onNext(message);
        }
      }

    };
    nameToDeserializerMap.put(msgMetaClass.getSimpleName(), messageDeserializer);
  }

  /**
   *
   * @param message
   */
  public static byte[] write(SpecificRecord message) {
    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
      final String name = message.getClass().getSimpleName();
      LOG.log(Level.INFO, "Serialing message: [" + name + "]");

      final MessageSerializer serializer = nameToSerializerMap.get(name);
      serializer.serialize(outputStream, message);

      return(outputStream.toByteArray());
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Failed to serialize avro message: " + e.getMessage());
    }
  }

  /**
   *
   * @param messageBytes
   * @param observer
   */
  public static void read(byte[] messageBytes, Object observer) {
    try (InputStream inputStream = new ByteArrayInputStream(messageBytes)) {
      // Binary decoder for both the header and the message.
      final BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);

      // Read the header message.
      final SpecificDatumReader<Header> headerReader = new SpecificDatumReader<>(Header.class);
      final Header header = headerReader.read(null, decoder);
      LOG.log(Level.INFO, "Deserializing message: [" + header.getClassName() + "]");

      // Get the appropriate deserializer and deserialize the message.
      final MessageDeserializer deserializer = nameToDeserializerMap.get(header.getClassName());
      deserializer.deserialize(decoder, observer);

    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Failed to deserialize avro message: " + e.getMessage());
    }
  }
}
