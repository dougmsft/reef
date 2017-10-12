/*
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

import org.apache.avro.specific.SpecificRecord;
import org.apache.reef.bridge.message.Acknowledgement;
import org.apache.reef.bridge.message.BridgeProtocol;
import org.apache.reef.bridge.message.SystemOnStart;
import org.apache.reef.util.MultiAsyncToSync;
import org.apache.reef.util.exception.InvalidIdentifierException;
import org.apache.reef.wake.impl.MultiObserverImpl;
import org.apache.reef.wake.remote.address.LocalAddressProvider;

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Implements the Avro message protocol between the Java and C# bridges.
 */
public final class JavaBridge extends MultiObserverImpl<JavaBridge> {
  private static final Logger LOG = Logger.getLogger(JavaBridge.class.getName());
  private static final int TIME_OUT = 20000;
  private final MultiAsyncToSync blocker = new MultiAsyncToSync(TIME_OUT, SECONDS);
  private final AtomicLong idCounter = new AtomicLong(0);
  private final Network network;

  /**
   * Inner class which sends its internal message when the call method is invoked.
   */
  private class MessageSender implements Callable<Boolean> {
    private final long identifier;
    private final SpecificRecord message;

    /**
     * Intialize the Message Sender with the specified message sequence identifier
     * and Avro message class.
     * @param identifier A long that contains the unique message sequence identifier.
     * @param message An Avro SpecifiedRecord instance whose subclass is an Avro
     *                message in the bridge protocol.
     */
    MessageSender(final long identifier, final SpecificRecord message) {
      this.identifier = identifier;
      this.message = message;
    }

    /**
     * Sends the internal message with the internal message sequence identifier.
     * @return Always returns true.
     */
    public Boolean call() {
      network.send(identifier, message);
      return true;
    }
  }

  /**
   * Implements the RPC interface to the C# side of the bridge.
   * @param localAddressProvider Used to find an available port on the local host.
   */
  @Inject
  public JavaBridge(final LocalAddressProvider localAddressProvider) {
    this.network = new Network(localAddressProvider, this);
  }

  /**
   * Retrieves the internet socket address of the Java side of the bridge.
   * @return An InetSocketAddress which contains the address of the Java
   * side of the bridge.
   */
  public InetSocketAddress getAddress() {
    return network.getAddress();
  }

  /**
   * Called when an error occurs in the MultiObserver base class.
   * @param error An exception reference that contains the error
   *              which occurred
   */
  public void onError(final Exception error) {
    LOG.log(Level.SEVERE, "Error received by Java bridge: ", error);
  }

  /**
   * Called when no more message processing is required.
   */
  public void onCompleted() {
    LOG.log(Level.INFO, "OnCompleted");
  }

  /**
   * Processes protocol messages from the C# side of the bridge.
   * @param identifier A long value which is the unique message identifier.
   * @param protocol A reference to the received Avro protocol message.
   */
  public void onNext(final long identifier, final BridgeProtocol protocol) {
    LOG.log(Level.INFO, "Received protocol message: [{0}] {1}", new Object[] {identifier, protocol.getOffset()});
  }

  /**
   * Releases the caller sleeping on the inpput acknowledgement message.
   * @param identifier A long value which is the unique message identifier.
   * @param acknowledgement The incoming acknowledgement message whose call will be released.
   * @throws InvalidIdentifierException The call identifier is invalid.
   * @throws InterruptedException Thread was interrupted by another thread.
   */
  public void onNext(final long identifier, final Acknowledgement acknowledgement)
        throws InvalidIdentifierException, InterruptedException {
    LOG.log(Level.INFO, "Received acknowledgement message for id = [{0}]", identifier);
    blocker.release(acknowledgement.getMessageIdentifier());
  }

  /**
   * Sends a SystemOnStart message to the CLR bridge and blocks the caller
   * until an acknowledgement message is received.
   */
  public void callClrSystemOnStartHandler() throws InvalidIdentifierException, InterruptedException {
    LOG.log(Level.INFO, "callClrSystemOnStartHandler called");
    final Date date = new Date();
    final long identifier = idCounter.getAndIncrement();
    blocker.block(identifier, new FutureTask<>(new MessageSender(identifier, new SystemOnStart(date.getTime()))));
  }
}

