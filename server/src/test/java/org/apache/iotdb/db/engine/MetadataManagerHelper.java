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
package org.apache.iotdb.db.engine;

import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.util.Collections;

public class MetadataManagerHelper {

  public static void initMetadata() {
    MManager mmanager = IoTDB.metaManager;
    mmanager.init();
    try {
      mmanager.setStorageGroup(new PartialPath("root.vehicle.d0"));
      mmanager.setStorageGroup(new PartialPath("root.vehicle.d1"));
      mmanager.setStorageGroup(new PartialPath("root.vehicle.d2"));

      CompressionType compressionType = TSFileDescriptor.getInstance().getConfig().getCompressor();

      mmanager.createTimeseries(
          new PartialPath("root.vehicle.d0.s0"),
          TSDataType.valueOf("INT32"),
          TSEncoding.PLAIN,
          compressionType,
          Collections.emptyMap());
      mmanager.createTimeseries(
          new PartialPath("root.vehicle.d0.s1"),
          TSDataType.valueOf("INT64"),
          TSEncoding.PLAIN,
          compressionType,
          Collections.emptyMap());
      mmanager.createTimeseries(
          new PartialPath("root.vehicle.d0.s2"),
          TSDataType.valueOf("FLOAT"),
          TSEncoding.PLAIN,
          compressionType,
          Collections.emptyMap());
      mmanager.createTimeseries(
          new PartialPath("root.vehicle.d0.s3"),
          TSDataType.valueOf("DOUBLE"),
          TSEncoding.PLAIN,
          compressionType,
          Collections.emptyMap());
      mmanager.createTimeseries(
          new PartialPath("root.vehicle.d0.s4"),
          TSDataType.valueOf("BOOLEAN"),
          TSEncoding.PLAIN,
          compressionType,
          Collections.emptyMap());
      mmanager.createTimeseries(
          new PartialPath("root.vehicle.d0.s5"),
          TSDataType.valueOf("TEXT"),
          TSEncoding.PLAIN,
          compressionType,
          Collections.emptyMap());

      mmanager.createTimeseries(
          new PartialPath("root.vehicle.d1.s0"),
          TSDataType.valueOf("INT32"),
          TSEncoding.PLAIN,
          compressionType,
          Collections.emptyMap());
      mmanager.createTimeseries(
          new PartialPath("root.vehicle.d1.s1"),
          TSDataType.valueOf("INT64"),
          TSEncoding.PLAIN,
          compressionType,
          Collections.emptyMap());
      mmanager.createTimeseries(
          new PartialPath("root.vehicle.d1.s2"),
          TSDataType.valueOf("FLOAT"),
          TSEncoding.PLAIN,
          compressionType,
          Collections.emptyMap());
      mmanager.createTimeseries(
          new PartialPath("root.vehicle.d1.s3"),
          TSDataType.valueOf("DOUBLE"),
          TSEncoding.PLAIN,
          compressionType,
          Collections.emptyMap());
      mmanager.createTimeseries(
          new PartialPath("root.vehicle.d1.s4"),
          TSDataType.valueOf("BOOLEAN"),
          TSEncoding.PLAIN,
          compressionType,
          Collections.emptyMap());
      mmanager.createTimeseries(
          new PartialPath("root.vehicle.d1.s5"),
          TSDataType.valueOf("TEXT"),
          TSEncoding.PLAIN,
          compressionType,
          Collections.emptyMap());

      mmanager.createTimeseries(
          new PartialPath("root.vehicle.d2.s0"),
          TSDataType.valueOf("INT32"),
          TSEncoding.PLAIN,
          compressionType,
          Collections.emptyMap());
      mmanager.createTimeseries(
          new PartialPath("root.vehicle.d2.s1"),
          TSDataType.valueOf("INT64"),
          TSEncoding.PLAIN,
          compressionType,
          Collections.emptyMap());
      mmanager.createTimeseries(
          new PartialPath("root.vehicle.d2.s2"),
          TSDataType.valueOf("FLOAT"),
          TSEncoding.PLAIN,
          compressionType,
          Collections.emptyMap());
      mmanager.createTimeseries(
          new PartialPath("root.vehicle.d2.s3"),
          TSDataType.valueOf("DOUBLE"),
          TSEncoding.PLAIN,
          compressionType,
          Collections.emptyMap());
      mmanager.createTimeseries(
          new PartialPath("root.vehicle.d2.s4"),
          TSDataType.valueOf("BOOLEAN"),
          TSEncoding.PLAIN,
          compressionType,
          Collections.emptyMap());
      mmanager.createTimeseries(
          new PartialPath("root.vehicle.d2.s5"),
          TSDataType.valueOf("TEXT"),
          TSEncoding.PLAIN,
          compressionType,
          Collections.emptyMap());

    } catch (Exception e) {
      throw new RuntimeException("Initialize the metadata manager failed", e);
    }
  }
}
