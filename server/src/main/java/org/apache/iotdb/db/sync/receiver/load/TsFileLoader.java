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
package org.apache.iotdb.db.sync.receiver.load;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.sync.PipeDataLoadException;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.OperateFilePlan;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/** This loader is used to load tsFiles. If .mods file exists, it will be loaded as well. */
public class TsFileLoader implements ILoader {
  private static final Logger logger = LoggerFactory.getLogger(TsFileLoader.class);
  private static PlanExecutor planExecutor;

  static {
    try {
      planExecutor = new PlanExecutor();
    } catch (QueryProcessException e) {
      logger.error(e.getMessage());
    }
  }

  private final File tsFile;
  // TODO(sync): use storage group to support auto create schema
  private final String storageGroup;

  public TsFileLoader(File tsFile, String storageGroup) {
    this.tsFile = tsFile;
    this.storageGroup = storageGroup;
  }

  @Override
  public void load() throws PipeDataLoadException {
    try {
      PhysicalPlan plan =
          new OperateFilePlan(
              tsFile,
              Operator.OperatorType.LOAD_FILES,
              true,
              IoTDBDescriptor.getInstance().getConfig().getDefaultStorageGroupLevel(),
              true);
      planExecutor.processNonQuery(plan);
    } catch (Exception e) {
      throw new PipeDataLoadException(e.getMessage());
    }
  }
}
