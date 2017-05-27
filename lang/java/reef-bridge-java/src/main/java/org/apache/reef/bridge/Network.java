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

import org.apache.avro.specific.SpecificRecord;
import org.apache.reef.annotations.audience.Private;
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

import java.net.InetSocketAddress;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Avro implementation of the java interface of CLR/Java bridge.
 */
final public class Network implements EventHandler<RemoteMessage<byte[]>>
{
  private static final Logger LOG = Logger.getLogger(Network.class.getName());

  private RemoteManager remoteManager;
  private InetSocketAddress inetSocketAddress;
  private EventHandler<byte[]> sender;
  private MultiObserver observer;

  /**
   * Set up the java bridge network.
   * @param localAddressProvider
   */
  public Network(final LocalAddressProvider localAddressProvider, final MultiObserver observer) {
    LOG.log(Level.INFO, "Initializing");

    this.observer = observer;
    Serializer.Initialize();

    try {
      String name = "JavaBridgeNetwork";
      int port = 0;
      boolean order = true;
      int retries = 3;
      int timeOut = 10000;

      // Instantiate a port provider.
      final Injector injector = Tang.Factory.getTang().newInjector();
      final TcpPortProvider tcpPortProvider = Tang.Factory.getTang().newInjector().getInstance(TcpPortProvider.class);

      // Instantiate a remote manager to handle java-C# bridge communication.
      final RemoteManagerFactory remoteManagerFactory = injector.getInstance(RemoteManagerFactory.class);
      remoteManager = remoteManagerFactory.getInstance(
        name, localAddressProvider.getLocalAddress(), port, new ByteCodec(),
        new LoggingEventHandler<Throwable>(), order, retries, timeOut,
        localAddressProvider, tcpPortProvider);

      // Get our address and port number.
      final RemoteIdentifier remoteIdentifier = remoteManager.getMyIdentifier();
      if (remoteIdentifier instanceof SocketRemoteIdentifier) {
        SocketRemoteIdentifier socketIdentifier = (SocketRemoteIdentifier)remoteIdentifier;
        inetSocketAddress = socketIdentifier.getSocketAddress();
      } else {
        throw new RuntimeException("Identifier is not a SocketRemoteIdentifier");
      }

      // Register as the message handler for any incoming messages.
      remoteManager.registerHandler(byte[].class, this);

    } catch (final Exception e) {
      e.printStackTrace();
      LOG.log(Level.SEVERE, "Initialization failed: " + e.getMessage());
    }
  }

  /**
   * Deserialize and direct incoming messages to the registered MuiltiObserver event handler.
   * @param message A RemoteMessage<byte[]> object which will be deserialized.
   */
  public void onNext(final RemoteMessage<byte[]> message) {
    LOG.log(Level.INFO,"Received remote message: " + message.toString());

    if (sender == null) {
      // Instantiate a network connection to the C# side of the bridge.
      // THERE COULD BE  A SECURITY ISSUE HERE WHERE SOMEONE SPOOFS THE
      // C# BRIDGE, WE RECEIVE IT FIRST, AND CONNECT TO THE SPOOFER,
      // THOUGH THE TIME WINDOW IS VERY SMALL.
      final RemoteIdentifier remoteIdentifier = message.getIdentifier();
      LOG.log(Level.INFO, "Connecting to: " + remoteIdentifier.toString());
      sender = remoteManager.getHandler(remoteIdentifier, byte[].class);
    }

    // Deserialize the message and invoke the appropriate processing method.
    Serializer.read(message.getMessage(), observer);
  }

  /**
   * Sends a message to the C# side of the bridge.
   * @param message An avro message class derived from SpecificRecord.
   */
  public void send(SpecificRecord message) {
    if (sender != null) {
      sender.onNext(Serializer.write(message));
    } else {
      LOG.log(Level.SEVERE,"Attempt to send message ["
        + message.getClass().getName() + "] before network is initialized");
    }
  }

  /**
   * Provides the ip address and port of the java bridge network.
   * @return A InetSockerAddress that contains the ip and port of the bridge network.
   */
  public InetSocketAddress getAddress() {
    return inetSocketAddress;
  }
}
