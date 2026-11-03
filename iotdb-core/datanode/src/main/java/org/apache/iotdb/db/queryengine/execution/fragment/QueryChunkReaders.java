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
package org.apache.iotdb.db.queryengine.execution.fragment;

import org.apache.iotdb.db.i18n.DataNodeQueryMessages;

import org.apache.tsfile.read.reader.IChunkReader;
import org.apache.tsfile.read.reader.IPageReader;

import java.io.IOException;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/** Keeps lazy pages' file contexts alive, releasing each owner once its last page is consumed. */
public final class QueryChunkReaders implements AutoCloseable {
  private final Map<IPageReader, Lease> pages = new IdentityHashMap<>();
  private boolean closed;

  public synchronized void retain(IChunkReader reader, List<IPageReader> pageReaders)
      throws IOException {
    if (closed) {
      reader.close();
      throw new IOException(
          DataNodeQueryMessages.EXCEPTION_QUERY_CHUNK_RESOURCES_ARE_ALREADY_CLOSED_CA6A0559);
    }
    if (pageReaders.isEmpty()) {
      reader.close();
      return;
    }
    Lease lease = new Lease(reader, pageReaders.size());
    for (IPageReader page : pageReaders) {
      pages.put(page, lease);
    }
  }

  public synchronized void release(IPageReader page) throws IOException {
    Lease lease = pages.remove(page);
    if (lease != null && --lease.remaining == 0) {
      lease.reader.close();
    }
  }

  @Override
  public synchronized void close() throws IOException {
    closed = true;
    Map<Lease, Boolean> owners = new IdentityHashMap<>();
    for (Lease lease : pages.values()) {
      owners.put(lease, Boolean.TRUE);
    }
    pages.clear();
    IOException failure = null;
    for (Lease lease : owners.keySet()) {
      try {
        lease.reader.close();
      } catch (IOException | RuntimeException e) {
        if (failure == null) {
          failure = e instanceof IOException ? (IOException) e : new IOException(e);
        } else {
          failure.addSuppressed(e);
        }
      }
    }
    if (failure != null) {
      throw failure;
    }
  }

  private static final class Lease {
    private final IChunkReader reader;
    private int remaining;

    private Lease(IChunkReader reader, int remaining) {
      this.reader = reader;
      this.remaining = remaining;
    }
  }
}
