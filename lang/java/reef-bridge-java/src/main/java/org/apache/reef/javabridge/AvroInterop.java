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
package org.apache.reef.javabridge;

import org.apache.reef.annotations.audience.Private;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.impl.NetworkService;
import org.apache.reef.io.network.impl.NetworkServiceParameters;
import org.apache.reef.io.network.naming.NameResolver;
import org.apache.reef.io.network.naming.NameResolverConfiguration;
import org.apache.reef.io.network.naming.NameServerParameters;
import org.apache.reef.io.network.util.StringCodec;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.impl.ObjectSerializableCodec;
import org.apache.reef.wake.remote.impl.SocketRemoteIdentifier;
import org.apache.reef.wake.remote.RemoteIdentifier;
import org.apache.reef.wake.remote.RemoteManager;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.wake.remote.RemoteManagerFactory;
import org.apache.reef.wake.remote.ports.TcpPortProvider;
import org.apache.reef.wake.impl.LoggingEventHandler;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.remote.transport.netty.MessagingTransportFactory;

import java.lang.Exception;
import java.net.InetSocketAddress;
import java.util.logging.Level;
import java.util.logging.Logger;


@Private
class MessageHandler<T> implements EventHandler<Message<T>> {
  private static final Logger LOG = Logger.getLogger(MessageHandler.class.getName());

  MessageHandler() {
  }

  @Override
  public void onNext(final Message<T> value) {
    LOG.log(Level.FINER, "JAVA BRIDGE RECEIVED MESSAGE: {0}", value);
  }
}


class ExceptionHandler implements EventHandler<Exception> {
  private static final Logger LOG = Logger.getLogger(ExceptionHandler.class.getName());

  @Override
  public void onNext(final Exception error) {
    System.err.println(error);
    LOG.log(Level.INFO, error.getMessage());
  }
}

/**
 * Avro implementation of the java interface of CLR/Java bridge.
 */
@Private
public final class AvroInterop {
  private static final Logger LOG = Logger.getLogger(AvroInterop.class.getName());

  private final String javaBridgeName = "JavaBridge";
  private final String clrBridgeName = "ClrBridge";

  private NameServer nameServer;
  private LocalAddressProvider localAddressProvider;
  private NetworkService<String> networkService;


  private RemoteManager remoteManager;
  private InetSocketAddress address;

  public AvroInterop(
      NameServer nameServer,
      LocalAddressProvider localAddressProvider)
  {
    LOG.log(Level.INFO, "Java bridge interop initializing");

    this.nameServer = nameServer;
    this.localAddressProvider = localAddressProvider;

    try {
      // Configure and instantiate the name resolver.
      final Configuration nameResolverConf =
        Tang.Factory.getTang().newConfigurationBuilder(NameResolverConfiguration.CONF
        .set(NameResolverConfiguration.NAME_SERVER_HOSTNAME, this.localAddressProvider.getLocalAddress())
        .set(NameResolverConfiguration.NAME_SERVICE_PORT, this.nameServer.getPort()).build()).build();
      final Injector injector = Tang.Factory.getTang().newInjector(nameResolverConf);
      final NameResolver nameResolver = injector.getInstance(NameResolver.class);

      // Instantiate an identifier factor.
      final IdentifierFactory identifierFactory = new StringIdentifierFactory();

      // Bind the identifier factory, name resolver, string codec, transport factory,
      // and exception handler to the network service configuration.
      injector.bindVolatileParameter(NetworkServiceParameters.NetworkServiceIdentifierFactory.class, identifierFactory);
      injector.bindVolatileInstance(NameResolver.class, nameResolver);
      injector.bindVolatileParameter(NetworkServiceParameters.NetworkServiceCodec.class, new StringCodec());
      injector.bindVolatileParameter(NetworkServiceParameters.NetworkServiceTransportFactory.class,
          injector.getInstance(MessagingTransportFactory.class));
      injector.bindVolatileParameter(NetworkServiceParameters.NetworkServiceExceptionHandler.class, new ExceptionHandler());

      // Instantiate the java bridge network service.
      final Injector injectorNs = injector.forkInjector();
      injectorNs.bindVolatileParameter(NetworkServiceParameters.NetworkServiceHandler.class, new MessageHandler<String>());
      networkService = injectorNs.getInstance(NetworkService.class);

      // Register the java bridge network service with the name server.
      final int port = networkService.getTransport().getListeningPort();
      final Identifier localIdentifier = identifierFactory.getNewInstance(javaBridgeName);
      networkService.registerId(localIdentifier);
      nameServer.register(localIdentifier, new InetSocketAddress(localAddressProvider.getLocalAddress(), port));

    } catch (final Exception e) {
      e.printStackTrace();
      LOG.log(Level.INFO, "Java bridge interop initialization failed: " + e.getMessage());
    }
  }

  private void Start()
  {
    final IdentifierFactory identifierFactory = new StringIdentifierFactory();
    Identifier destId = identifierFactory.getNewInstance(clrBridgeName);
    try (final Connection<String> conn = networkService.newConnection(destId)) {
      conn.open();
      for (int count = 0; count < 3; ++count) {
        conn.write("hello! " + count);
      }
    } catch (final NetworkException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

  }

}
