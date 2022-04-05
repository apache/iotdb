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
package org.apache.iotdb.db.engine.compaction.cross.rewrite.task;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.CompactionUtils;
import org.apache.iotdb.db.engine.compaction.writer.AbstractCompactionWriter;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.metadata.idtable.IDTableManager;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * This class is used to implement reading the measurements and writing to the target files in
 * parallel in the compaction. Currently, it only works for nonAligned data in cross space
 * compaction and unseq inner space compaction.
 */
public class SubCompactionTask implements Callable {
  private static final Logger logger =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);
  private final String device;
  private final Set<String> measurementList;
  private final QueryContext queryContext;
  private final QueryDataSource queryDataSource;
  private final AbstractCompactionWriter compactionWriter;
  private final int taskId;

  public SubCompactionTask(
      String device,
      Set<String> measurementList,
      QueryContext queryContext,
      QueryDataSource queryDataSource,
      AbstractCompactionWriter compactionWriter,
      int taskId) {
    this.device = device;
    this.measurementList = measurementList;
    this.queryContext = queryContext;
    this.queryDataSource = queryDataSource;
    this.compactionWriter = compactionWriter;
    this.taskId = taskId;
  }

  @Override
  public Void call() throws Exception {
    for (String measurement : measurementList) {
      List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
      try {
        if (IoTDBDescriptor.getInstance().getConfig().isEnableIDTable()) {
          measurementSchemas.add(IDTableManager.getInstance().getSeriesSchema(device, measurement));
        } else {
          measurementSchemas.add(
              IoTDB.schemaProcessor.getSeriesSchema(new PartialPath(device, measurement)));
        }
      } catch (PathNotExistException e) {
        logger.info("A deleted path is skipped: {}", e.getMessage());
        continue;
      }

      IBatchReader dataBatchReader =
          CompactionUtils.constructReader(
              device,
              Collections.singletonList(measurement),
              measurementSchemas,
              measurementList,
              queryContext,
              queryDataSource,
              false);

      if (dataBatchReader.hasNextBatch()) {
        compactionWriter.startMeasurement(measurementSchemas, taskId);
        CompactionUtils.writeWithReader(compactionWriter, dataBatchReader, taskId);
        compactionWriter.endMeasurement(taskId);
      }
    }
    return null;
  }
}
