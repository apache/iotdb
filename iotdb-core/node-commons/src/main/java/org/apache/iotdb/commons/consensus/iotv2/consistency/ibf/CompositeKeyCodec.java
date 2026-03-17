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

package org.apache.iotdb.commons.consensus.iotv2.consistency.ibf;

/**
 * Codec for the 8-byte composite key used in IBF cells. Layout:
 *
 * <pre>
 *   [deviceIndex: 2 bytes][measurementIndex: 2 bytes][timestamp_bucket_offset: 4 bytes]
 * </pre>
 *
 * The deviceIndex and measurementIndex are ordinal indices (sorted order) agreed upon by Leader and
 * Follower during the NEGOTIATE_KEY_MAPPING phase. The timestamp_bucket_offset encodes (timestamp -
 * timeBucketStart) in units of the minimum timestamp resolution.
 */
public final class CompositeKeyCodec {

  public static final long DEFAULT_TIMESTAMP_RESOLUTION = 1L;
  public static final int MAX_DEVICE_INDEX = 0xFFFF;
  public static final int MAX_MEASUREMENT_INDEX = 0xFFFF;

  private CompositeKeyCodec() {}

  /**
   * Encode a data point's location into a composite key.
   *
   * @param deviceIndex ordinal index of the device within the partition
   * @param measurementIndex ordinal index of the measurement within the device
   * @param timestamp the data point's timestamp
   * @param bucketStart the start of the time bucket
   * @param timestampResolution minimum timestamp resolution
   * @return 8-byte composite key
   */
  public static long encode(
      int deviceIndex,
      int measurementIndex,
      long timestamp,
      long bucketStart,
      long timestampResolution) {
    int tsOffset = (int) ((timestamp - bucketStart) / timestampResolution);
    return ((long) (deviceIndex & 0xFFFF) << 48)
        | ((long) (measurementIndex & 0xFFFF) << 32)
        | (tsOffset & 0xFFFFFFFFL);
  }

  public static int extractDeviceIndex(long compositeKey) {
    return (int) ((compositeKey >>> 48) & 0xFFFF);
  }

  public static int extractMeasurementIndex(long compositeKey) {
    return (int) ((compositeKey >>> 32) & 0xFFFF);
  }

  public static int extractTimestampOffset(long compositeKey) {
    return (int) (compositeKey & 0xFFFFFFFFL);
  }

  /**
   * Recover the original timestamp from the composite key.
   *
   * @param compositeKey the encoded key
   * @param bucketStart the time bucket start
   * @param timestampResolution the timestamp resolution
   * @return the original timestamp
   */
  public static long recoverTimestamp(
      long compositeKey, long bucketStart, long timestampResolution) {
    int tsOffset = extractTimestampOffset(compositeKey);
    return bucketStart + (long) tsOffset * timestampResolution;
  }
}
