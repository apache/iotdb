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

package org.apache.iotdb.consensus.common.request;

/**
 * Conservative classification supplied by the application, never inferred from a batch's first row.
 */
public final class RequestSemantics {
  public enum Kind {
    DATA,
    BARRIER
  }

  private static final RequestSemantics BARRIER = new RequestSemantics(Kind.BARRIER, 0, false, 0);
  private final Kind kind;
  private final long timestamp;
  private final boolean hasSeries;
  private final long seriesKey;

  private RequestSemantics(Kind kind, long timestamp, boolean hasSeries, long seriesKey) {
    this.kind = kind;
    this.timestamp = timestamp;
    this.hasSeries = hasSeries;
    this.seriesKey = seriesKey;
  }

  /** A data write whose series is unknown: it may overwrite any point at its timestamp. */
  public static RequestSemantics data(long timestamp) {
    return new RequestSemantics(Kind.DATA, timestamp, false, 0);
  }

  /**
   * A data write that only touches points of one series at one timestamp. Two such writes can
   * overwrite each other only if both their series keys and timestamps are equal.
   */
  public static RequestSemantics data(long timestamp, long seriesKey) {
    return new RequestSemantics(Kind.DATA, timestamp, true, seriesKey);
  }

  /**
   * A 64-bit FNV-1a hash of a series (device) name. Equal names give equal keys; two different
   * names that collide are only treated as one series, which is conservative.
   */
  public static long seriesKeyOf(String series) {
    long hash = 0xcbf29ce484222325L;
    for (int i = 0; i < series.length(); i++) {
      hash ^= series.charAt(i);
      hash *= 0x100000001b3L;
    }
    return hash;
  }

  public static RequestSemantics barrier() {
    return BARRIER;
  }

  public Kind getKind() {
    return kind;
  }

  public long getTimestamp() {
    if (kind != Kind.DATA) {
      throw new IllegalStateException("A barrier has no single data timestamp");
    }
    return timestamp;
  }

  public boolean hasSeriesKey() {
    return hasSeries;
  }

  public long getSeriesKey() {
    if (!hasSeries) {
      throw new IllegalStateException("This write has no single series");
    }
    return seriesKey;
  }
}
