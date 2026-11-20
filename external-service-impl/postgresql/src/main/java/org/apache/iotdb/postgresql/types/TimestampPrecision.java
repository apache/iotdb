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

package org.apache.iotdb.postgresql.types;

/** The table model's timestamp precision ({@code timestamp_precision}). */
public enum TimestampPrecision {
  MS,
  US,
  NS;

  /** The precision configured as {@code ms}, {@code us} or {@code ns}. */
  public static TimestampPrecision of(String configured) {
    switch (configured == null ? "ms" : configured.trim().toLowerCase(java.util.Locale.ROOT)) {
      case "us":
        return US;
      case "ns":
        return NS;
      default:
        return MS;
    }
  }

  /** Microseconds since 1970 (PostgreSQL's resolution; nanoseconds are truncated toward -∞). */
  public long toEpochMicros(long value) {
    switch (this) {
      case MS:
        return Math.multiplyExact(value, 1000L);
      case NS:
        return Math.floorDiv(value, 1000L);
      default:
        return value;
    }
  }

  /** A value in this precision from microseconds since 1970. */
  public long fromEpochMicros(long micros) {
    switch (this) {
      case MS:
        return Math.floorDiv(micros, 1000L);
      case NS:
        return Math.multiplyExact(micros, 1000L);
      default:
        return micros;
    }
  }
}
