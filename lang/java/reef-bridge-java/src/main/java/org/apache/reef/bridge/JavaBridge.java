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

import org.apache.reef.bridge.message.Protocol;
import org.apache.reef.bridge.message.SystemOnStart;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.net.InetSocketAddress;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 */
public final class JavaBridge extends MultiObserverImpl<JavaBridge> {
  private static final Logger LOG = Logger.getLogger(JavaBridge.class.getName());
  private final Network network;

  public JavaBridge(final LocalAddressProvider localAddressProvider) {
    this.network = new Network(localAddressProvider, this);
  }

  public void onNext(Protocol protocol) {
    LOG.log(Level.INFO,"+++++++Received protocol message: " + protocol.getOffset().toString());
  }

  public void onError(final Exception error) {
    throw new NotImplementedException();
  }

  public void onCompleted() {
    throw new NotImplementedException();
  }

  public InetSocketAddress getAddress() {
    return network.getAddress();
  }

  public void callClrSystemOnStartHandler() {
    Date date = new Date();
    network.send(new SystemOnStart(date.getTime()));
  }

}
