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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.validator;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.compaction.constant.CompactionValidationLevel;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import java.io.IOException;
import java.util.List;

public interface CompactionValidator {
  boolean validateCompaction(
      TsFileManager manager,
      List<TsFileResource> targetTsFileList,
      String storageGroupName,
      long timePartition,
      boolean isInnerUnSequenceSpaceTask)
      throws IOException;

  static CompactionValidator getInstance() {
    CompactionValidationLevel level =
        IoTDBDescriptor.getInstance().getConfig().getCompactionValidationLevel();
    switch (level) {
      case NONE:
        return NoneCompactionValidator.getInstance();
      case RESOURCE_ONLY:
        return ResourceOnlyCompactionValidator.getInstance();
      default:
        return ResourceAndTsfileCompactionValidator.getInstance();
    }
  }
}
