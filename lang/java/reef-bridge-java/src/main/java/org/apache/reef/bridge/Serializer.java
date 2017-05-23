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

import org.apache.reef.bridge.message.*;
import org.apache.avro.specific.SpecificRecordBase;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import io.github.lukehutch.fastclasspathscanner.FastClasspathScanner;
import io.github.lukehutch.fastclasspathscanner.scanner.ScanResult;

final public class Serializer {
  private static final Logger LOG = Logger.getLogger(Serializer.class.getName());
  private static Header header = new Header();

  private Serializer() { }

  public static void Initialize() {
    LOG.log(Level.INFO, "Start: Serializer.Initialize");

    ScanResult scanResult = new FastClasspathScanner("org.apache.reef.bridge.message").scan();
    List<String> scanNames = scanResult.getNamesOfSubclassesOf(SpecificRecordBase.class);
    List<Class<? extends SpecificRecordBase>> messageClasses
      = (List<Class<? extends SpecificRecordBase>>)scanResult.classNamesToClassRefs(scanNames);

    LOG.log(Level.INFO, "!!!NUMBER OF MESSAGES = " + Integer.toString(messageClasses.size()));
    for (Class<? extends SpecificRecordBase> cls : messageClasses) {
      LOG.log(Level.INFO, "Message class: " + cls.getName());
    }

    LOG.log(Level.INFO, "End: Serializer.Initialize");
  }


}
