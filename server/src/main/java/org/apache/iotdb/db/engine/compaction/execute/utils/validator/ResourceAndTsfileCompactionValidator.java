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

package org.apache.iotdb.db.engine.compaction.execute.utils.validator;

import org.apache.iotdb.db.engine.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

import java.io.IOException;
import java.util.List;

public class ResourceAndTsfileCompactionValidator implements CompactionValidator {

  private ResourceAndTsfileCompactionValidator() {}

  public static ResourceAndTsfileCompactionValidator getInstance() {
    return ResourceAndTsfileCompactionValidatorHolder.INSTANCE;
  }

  @Override
  public boolean validateCompaction(
      TsFileManager manager,
      List<TsFileResource> targetTsFileList,
      String storageGroupName,
      long timePartition)
      throws IOException {
    return CompactionUtils.validateTsFileResources(manager, storageGroupName, timePartition)
        && CompactionUtils.validateTsFiles(targetTsFileList);
  }

  private static class ResourceAndTsfileCompactionValidatorHolder {
    private static final ResourceAndTsfileCompactionValidator INSTANCE =
        new ResourceAndTsfileCompactionValidator();
  }
}
