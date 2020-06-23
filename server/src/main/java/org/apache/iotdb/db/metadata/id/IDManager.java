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
package org.apache.iotdb.db.metadata.id;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iotdb.db.exception.metadata.MetadataException;

/**
 * ID generator for storage groups, devices, and measurements
 */
public class IDManager {

  private static int storageGroupIDLength = 12; //must be not greater than 32
  private static int deviceIDLength = 32; //must be not greater than 32
  private static int measurementLength = 20; //must be not greater than 32

  private static int maxSGID;
  private static int maxDeivceID;
  private static int maxMeasurementID;
  static {
    if (storageGroupIDLength == 32) {
      maxSGID = 0xFFFFFFFF;
    } else {
      maxSGID = 1 << storageGroupIDLength - 1;
    }

    if (deviceIDLength == 32) {
      maxDeivceID = 0xFFFFFFFF;
    } else {
      maxDeivceID = 1 << deviceIDLength - 1;
    }

    if (measurementLength == 32) {
      maxMeasurementID = 0xFFFFFFFF;
    } else {
      maxMeasurementID = 1 << measurementLength - 1;
    }
  }

  private static AtomicInteger sgGenerator = new AtomicInteger(0);
  private static AtomicInteger deviceGenerator = new AtomicInteger(0);
  private static AtomicInteger measurementGenerator = new AtomicInteger(0);


  public static int newSGNumber() throws MetadataException {
    if (sgGenerator.get() == maxSGID) {
      throw new MetadataException("too many storage groups: {}", sgGenerator.get());
    }
    return sgGenerator.incrementAndGet();
  }

  public static int newDeviceNumber() throws MetadataException {
    if (deviceGenerator.get() == maxDeivceID) {
      throw new MetadataException("too many devices: {}", deviceGenerator.get());
    }
    return deviceGenerator.incrementAndGet();
  }

  public static int newMeasurementNumber() throws MetadataException  {
    if (measurementGenerator.get() == maxMeasurementID) {
      throw new MetadataException("too many measurements: {}", measurementGenerator.get());
    }
    return measurementGenerator.incrementAndGet();
  }

  public static long newID() throws MetadataException {
    return (0L | ((long) newSGNumber()) << (deviceIDLength + measurementLength) | ((long) newDeviceNumber()) << measurementLength | newMeasurementNumber());
  }

  public static int getStorageGroupID(long fullId) {
    return (int)(fullId >>> (deviceIDLength + measurementLength));
  }

  public static int getDeviceID(long fullId) {
    return (int)((fullId << storageGroupIDLength) >>> (storageGroupIDLength + measurementLength));
  }

  public static int getMeasurementID(long fullId) {
    return (int)((fullId << storageGroupIDLength + deviceIDLength) >>> (storageGroupIDLength + deviceIDLength));
  }


}
