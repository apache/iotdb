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

package org.apache.iotdb.db.engine.compaction.innerSpaceCompaction.no;

import org.apache.iotdb.db.engine.compaction.TsFileManagement;
import org.apache.iotdb.db.engine.compaction.innerSpaceCompaction.InnerSpaceCompactionExecutor;
import org.apache.iotdb.db.engine.compaction.innerSpaceCompaction.level.LevelCompactionExecutor;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class NoCompactionTsFileManagement extends InnerSpaceCompactionExecutor {
  private static final Logger logger = LoggerFactory.getLogger(LevelCompactionExecutor.class);

  public NoCompactionTsFileManagement(TsFileManagement tsFileManagement) {
    super(tsFileManagement);
  }

  @Override
  public void recover() {
    logger.info("no recover logic");
  }

  @Override
  public void doInnerSpaceCompaction(
      List<List<TsFileResource>> mergeResources, boolean sequence, long timePartition) {
    logger.info("no compaction logic");
  }
}
