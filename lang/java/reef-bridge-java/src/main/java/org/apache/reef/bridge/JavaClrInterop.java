/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.bridge;

import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.bridge.message.Header;
import org.apache.reef.bridge.message.Protocol;
import org.apache.reef.bridge.message.SystemOnStart;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.RemoteMessage;
import org.apache.reef.wake.remote.impl.ByteCodec;
import org.apache.reef.wake.remote.impl.SocketRemoteIdentifier;
import org.apache.reef.wake.remote.RemoteIdentifier;
import org.apache.reef.wake.remote.RemoteManager;
import org.apache.reef.wake.remote.RemoteManagerFactory;
import org.apache.reef.wake.remote.ports.TcpPortProvider;
import org.apache.reef.wake.impl.LoggingEventHandler;
import org.apache.reef.wake.remote.address.LocalAddressProvider;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.lang.CharSequence;
import java.lang.Integer;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Avro implementation of the java interface of CLR/Java bridge.
 */
@Private
public final class JavaClrInterop implements EventHandler<RemoteMessage<byte[]>> {
  private static final Logger LOG = Logger.getLogger(JavaClrInterop.class.getName());

  private RemoteManager remoteManager;
  private InetSocketAddress inetSocketAddress;
  private EventHandler<byte[]> sender;

  /**
   *
   * @param localAddressProvider
   */
  public JavaClrInterop(final LocalAddressProvider localAddressProvider) {
    LOG.log(Level.INFO, "Java bridge interop initializing");
    Serializer.Initialize();

    try {
      String name = "JavaClrInterop";
      int port = 0;
      boolean order = true;
      int retries = 3;
      int timeOut = 10000;

      final Injector injector = Tang.Factory.getTang().newInjector();
      TcpPortProvider tcpPortProvider = Tang.Factory.getTang().newInjector().getInstance(TcpPortProvider.class);

      RemoteManagerFactory remoteManagerFactory = injector.getInstance(RemoteManagerFactory.class);
      remoteManager = remoteManagerFactory.getInstance(
        name, localAddressProvider.getLocalAddress(), port, new ByteCodec(),
        new LoggingEventHandler<Throwable>(), order, retries, timeOut,
        localAddressProvider, tcpPortProvider);

      RemoteIdentifier remoteIdentifier = remoteManager.getMyIdentifier();
      if (remoteIdentifier instanceof SocketRemoteIdentifier) {
        SocketRemoteIdentifier socketIdentifier = (SocketRemoteIdentifier)remoteIdentifier;
        inetSocketAddress = socketIdentifier.getSocketAddress();
      } else {
        throw new RuntimeException("Remote manager identifier is not a SocketRemoteIdentifier");
      }

      remoteManager.registerHandler(byte[].class, this);

    } catch (final Exception e) {
      e.printStackTrace();
      LOG.log(Level.INFO, "Java bridge interop initialization failed: " + e.getMessage());
    }
  }

  /**
   *
   * @param message
   */
  public void onNext(final RemoteMessage<byte[]> message) {

    List<CharSequence> classList = null;
    int index = -1;
    LOG.log(Level.INFO, "!!!!!!!Java bridge received message: " + message.toString());

    // Setup an input stream from the bytes.
    try (InputStream inputStream = new ByteArrayInputStream(message.getMessage())){
      // Feed the bytes to a binary decoder.
      final BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream,null);
      // Read the header message to find out what follows.
      final SpecificDatumReader<Header> headerReader = new SpecificDatumReader<>(Header.class);
      Header header = headerReader.read(null, decoder);

      LOG.log(Level.INFO, "!!!!!!!Message header = " +  header.getIdentifier().toString());

      if (header.getIdentifier() == 100) {
        // The next message is a Protocol message as expected.
        final SpecificDatumReader<Protocol> protocolReader = new SpecificDatumReader<>(Protocol.class);
        Protocol protocol = protocolReader.read(null, decoder);
        if (protocol == null) {
          LOG.log(Level.INFO, "!!!!!!!Failed to read Protocol message");
        } else {
          index = protocol.getOffset();
          LOG.log(Level.INFO, "!!!!!!!Protocol message: " + Integer.toString(index));
        }

        classList = protocol.getNames();
        LOG.log(Level.INFO, "!!!!!!!Protocol list size: " + Integer.toString(classList.size()));

        for (CharSequence name : classList) {
          LOG.log(Level.INFO, "CLASS NAME: " + name.toString());
        }

      } else {
        LOG.log(Level.INFO, "!!!!!!!Do not receive expected Protocol message");
      }
    } catch(Exception e) {
      LOG.log(Level.INFO, "!!!!!!!Error decoding Protocol message: " + e.getMessage());
    }

    RemoteIdentifier remoteIdentifier =  message.getIdentifier();
    LOG.log(Level.INFO, "!!!!!!!Java bridge connecting to: " + remoteIdentifier.toString());
    sender = remoteManager.getHandler(remoteIdentifier, byte[].class);

    //String className = SystemOnStart.class.getName();
    Integer msgId = index; //classMap.get(className.subSequence(0,className.length()));

    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
      BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);

      DatumWriter<Header> headerWriter = new SpecificDatumWriter<>(Header.class);
      DatumWriter<SystemOnStart> sysOnStartWriter = new SpecificDatumWriter<>(SystemOnStart.class);

      headerWriter.write(new Header(msgId, "SystemOnStart"), encoder);
      sysOnStartWriter.write(new SystemOnStart(), encoder);
      encoder.flush();

      sender.onNext(outputStream.toByteArray());

    } catch(Exception e) {
      LOG.log(Level.INFO, "!!!!!!!Error sending SystemOnStart message: " + e.getMessage());
    }
  }

  public InetSocketAddress getAddress() {
    return inetSocketAddress;
  }
}
