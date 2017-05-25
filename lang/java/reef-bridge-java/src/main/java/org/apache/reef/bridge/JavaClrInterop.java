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

import org.apache.reef.annotations.audience.Private;
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

import java.lang.CharSequence;
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
  private EventHandler<byte[]> sender = null;

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

  public void onNext(Protocol protocol) {
    LOG.log(Level.INFO,"!!!!!!!Java bridge received protocol message: " + protocol.toString());
  }

  /**
   *
   * @param message
   */
  public void onNext(final RemoteMessage<byte[]> message) {

    List<CharSequence> classList = null;
    int index = -1;
    LOG.log(Level.INFO,"!!!!!!!Java bridge received message: " + message.toString());
    // Deserialize the message and invoke the appropriate processing method.
    Serializer.read(message.getMessage(),this);

    if (sender == null) {
      // Instantiate a network connection to the C# side of the bridge.
      RemoteIdentifier remoteIdentifier = message.getIdentifier();
      LOG.log(Level.INFO, "!!!!!!!Java bridge connecting to: " + remoteIdentifier.toString());
      sender = remoteManager.getHandler(remoteIdentifier, byte[].class);
    }
    sender.onNext(Serializer.write(new SystemOnStart());
  }

  public InetSocketAddress getAddress() {
    return inetSocketAddress;
  }
}
