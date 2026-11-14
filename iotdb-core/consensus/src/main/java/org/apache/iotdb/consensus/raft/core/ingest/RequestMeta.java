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

package org.apache.iotdb.consensus.raft.core.ingest;

/**
 * What the ingestion side knows about a request before it becomes an entry: its timestamp range
 * (TRaft) or nothing ({@link #OPAQUE}, which TRaft turns into a BARRIER partition), plus an opaque
 * protocol extension {@link #ext} (ParallelRaft: the encoded conflict keys; null = none).
 */
public final class RequestMeta {

  public static final long NO_TIME = Long.MIN_VALUE;
  public static final RequestMeta OPAQUE = new RequestMeta(NO_TIME, NO_TIME, true);

  public final long ts;
  public final long tsMax;
  public final boolean barrier;

  /** Protocol extension extracted before ingestion (forwarded with the write); nullable. */
  public final byte[] ext;

  public RequestMeta(long ts, long tsMax, boolean barrier) {
    this(ts, tsMax, barrier, null);
  }

  public RequestMeta(long ts, long tsMax, boolean barrier, byte[] ext) {
    this.ts = ts;
    this.tsMax = tsMax;
    this.barrier = barrier;
    this.ext = ext;
  }

  /** The same metadata with another extension (null removes it). */
  public RequestMeta withExt(byte[] newExt) {
    return new RequestMeta(ts, tsMax, barrier, newExt);
  }

  public static RequestMeta timed(long ts) {
    return new RequestMeta(ts, ts, false);
  }

  public static RequestMeta timed(long ts, long tsMax) {
    return new RequestMeta(ts, tsMax, false);
  }

  public boolean isBarrier() {
    return barrier;
  }

  @Override
  public String toString() {
    String base = barrier ? "opaque" : "ts=" + ts + (tsMax != ts ? ".." + tsMax : "");
    return ext == null ? base : base + ",ext=" + ext.length + "B";
  }
}
