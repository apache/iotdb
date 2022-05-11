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
package org.apache.iotdb.db.engine.snapshot;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.DataRegion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class SnapshotLoader {
  private Logger LOGGER = LoggerFactory.getLogger(SnapshotLoader.class);
  private String storageGroupName;
  private String dataDirPath;
  private String dataRegionId;

  public SnapshotLoader(String dataDirPath, String storageGroupName, String dataRegionId) {
    this.dataDirPath = dataDirPath;
    this.storageGroupName = storageGroupName;
    this.dataRegionId = dataRegionId;
  }

  public DataRegion loadSnapshot() {
    File seqDataDir =
        new File(
            dataDirPath
                + File.separator
                + IoTDBConstant.SEQUENCE_FLODER_NAME
                + File.separator
                + storageGroupName
                + File.separator
                + dataRegionId);
    File unseqDataDir =
        new File(
            dataDirPath
                + File.separator
                + IoTDBConstant.UNSEQUENCE_FLODER_NAME
                + File.separator
                + storageGroupName
                + File.separator
                + dataRegionId);
    if (!seqDataDir.exists() && !unseqDataDir.exists()) {
      return null;
    }
    try {
      return DataRegion.recoverFromSnapshot(
          storageGroupName,
          dataRegionId,
          dataDirPath,
          StorageEngine.getInstance().getSystemDir() + File.separator + storageGroupName);
    } catch (Exception e) {
      LOGGER.error("Exception occurs while load snapshot from {}", seqDataDir, e);
      return null;
    }
  }
}
