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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.storageengine.dataregion.wal.io;

import org.apache.iotdb.db.i18n.StorageEngineMessages;

import java.io.IOException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

/** A format/policy failure that recovery must never downgrade to tail truncation or skip. */
public class WALFormatException extends IOException {
  public WALFormatException(String message) {
    super(message);
  }

  public WALFormatException(String message, Throwable cause) {
    super(message, cause);
  }

  public static WALFormatException unsupported() {
    return new WALFormatException(
        StorageEngineMessages.EXCEPTION_UNSUPPORTED_OR_UNAVAILABLE_WAL_FORMAT_4629CFE4);
  }

  public static boolean isFatal(Throwable failure) {
    Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    for (Throwable current = failure;
        current != null && seen.add(current);
        current = current.getCause()) {
      if (current instanceof WALFormatException) {
        return true;
      }
    }
    return false;
  }
}
