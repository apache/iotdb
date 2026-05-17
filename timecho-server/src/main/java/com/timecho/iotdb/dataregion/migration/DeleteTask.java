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

package com.timecho.iotdb.dataregion.migration;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteTask implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(DeleteTask.class);

  private final TsFileResource tsFileResource;

  public DeleteTask(TsFileResource tsFileResource) {
    this.tsFileResource = tsFileResource;
  }

  @Override
  public void run() {
    try {
      DataRegion dataRegion =
          StorageEngine.getInstance()
              .getDataRegion(new DataRegionId(Integer.parseInt(tsFileResource.getDataRegionId())));
      dataRegion.removeTsFile(tsFileResource.getTsFile());
    } finally {
      // try to set the final status to NORMAL to avoid migrate failure
      // TODO: this setting may occur side effects
      tsFileResource.setStatus(TsFileResourceStatus.NORMAL);
    }
    logger.info(
        TimechoServerMessages.SUCCESSFULLY_DELETE_TSFILE_BY_SPACE_TL, tsFileResource.getTsFile());
  }
}
