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

package org.apache.iotdb.postgresql.session;

import java.security.SecureRandom;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Process ids and secret keys of the open connections (BackendKeyData), so that a CancelRequest on
 * a new connection can find the connection whose query it cancels.
 */
public final class CancelRegistry {

  private static final SecureRandom RANDOM = new SecureRandom();

  private static final class Entry {
    final int secretKey;
    final Runnable cancel;

    Entry(int secretKey, Runnable cancel) {
      this.secretKey = secretKey;
      this.cancel = cancel;
    }
  }

  private final AtomicInteger nextProcessId = new AtomicInteger(1000 + RANDOM.nextInt(50_000));
  private final Map<Integer, Entry> entries = new ConcurrentHashMap<>();

  /** A registered connection: its process id and secret key. */
  public static final class Key {
    private final int processId;
    private final int secretKey;

    Key(int processId, int secretKey) {
      this.processId = processId;
      this.secretKey = secretKey;
    }

    public int processId() {
      return processId;
    }

    public int secretKey() {
      return secretKey;
    }
  }

  /** Registers a connection; {@code cancel} stops its running query, if any. */
  public Key register(Runnable cancel) {
    int processId = nextProcessId.getAndIncrement();
    int secretKey = RANDOM.nextInt();
    entries.put(processId, new Entry(secretKey, cancel));
    return new Key(processId, secretKey);
  }

  public void unregister(Key key) {
    entries.remove(key.processId);
  }

  /**
   * Cancels the query of the connection with this process id if the secret key matches; an unknown
   * id or a wrong key does nothing (as in PostgreSQL, nothing is reported).
   */
  public void cancel(int processId, int secretKey) {
    Entry entry = entries.get(processId);
    if (entry != null && entry.secretKey == secretKey) {
      entry.cancel.run();
    }
  }

  public int size() {
    return entries.size();
  }
}
