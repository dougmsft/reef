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

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The MultiObserverImpl class uses reflection to discover which onNext()
 * event processing methods are defined and then map events to then.
 * @param <TSubCls> The subclass derived from MultiObserverImpl.
 */
public abstract class MultiObserverImpl<TSubCls> implements MultiObserver {
  private static final Logger LOG = Logger.getLogger(MultiObserverImpl.class.getName());
  private final Map<String, Method> methodMap = new HashMap<>();
  private boolean initialized = false;

  /**
   * Use reflection to discover all of the event processing methods in TSubCls
   * and setup a means to direct calls from the generic event onNext method defined
   * in the MultiObserver interface to specific concrete event onNext methods.
   */
  private void initialize() {
    // Get all of the onNext event processing methods.
    final Class<?> cls = this.getClass();
    final Method[] methods = cls.getMethods();

    // Iterate across the methods and build a hash map of class names to reflection methods.
    for (int idx = 0; idx < methods.length; ++idx)  {
      if (methods[idx].getName() == "onNext" && methods[idx].getDeclaringClass().equals(this.getClass())) {
        // This is an onNext method defined in TSubCls
        final Class<?>[] types = methods[idx].getParameterTypes();
        if (types.length == 1) {
          methodMap.put(types[0].getName(), methods[idx]);
        }
      }
    }
  }

  /**
   * Called when an event is received that does not have an onNext method definition
   * in TSubCls. Override in TSubClas to handle the error.
   * @param event A reference to an object which is an event not handled by TSubCls.
   */
  public void unimplemented(final Object event) {
    LOG.log(Level.INFO,"Unimplemented event: " + event.getClass().getName());
  }

  /**
   * Generic event onNext method in the base interface which maps the call to a concrete
   * event onNext method in TSubCls if one exists otherwise unimplemented is invoked.
   * @param event An event of type TEvent which will be sent to TSubCls as appropriate.
   * @param <TEvent> The type of the event being processed.
   */
  @Override
  public <TEvent> void onNext(final TEvent event) {
    if (!initialized) {
      initialize();
      initialized = true;
    }
    try {
      // Get the reflection method for this call.
      Method onNext = methodMap.get(event.getClass().getName());
      if (onNext != null) {
        // Process the event.
        onNext.invoke((TSubCls) this, event);
      } else {
        // Log the unprocessed event.
        unimplemented(event);
      }
    } catch(Exception e) {
      LOG.log(Level.SEVERE,"Caught exception dispatching onNext() event: " + e.getMessage());
    }
  }
}
