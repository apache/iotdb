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

package org.apache.iotdb.db.engine.cq;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.ContinuousQueryException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.logical.crud.QueryOperator;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimePlan;
import org.apache.iotdb.db.qp.physical.sys.CreateContinuousQueryPlan;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.TypeInferenceUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class ContinuousQuerySchemaCheckTask extends ContinuousQueryTask {

  public static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  public ContinuousQuerySchemaCheckTask(
      CreateContinuousQueryPlan continuousQueryPlan, long windowEndTimestamp) {
    super(continuousQueryPlan, windowEndTimestamp);
  }

  /** we only do some checks here. we don't write any data. */
  @Override
  protected void doInsert(
      String sql, QueryOperator queryOperator, GroupByTimePlan queryPlan, QueryDataSet queryDataSet)
      throws MetadataException, StorageEngineException, IOException {
    Set<PartialPath> targetPaths = new HashSet<>(generateTargetPaths(queryDataSet.getPaths()));
    checkTargetPathNumber(queryDataSet, targetPaths);
    checkTargetPathDataType(queryPlan, targetPaths);
    tryExecuteQueryOnce(queryDataSet);
  }

  private void checkTargetPathNumber(QueryDataSet queryDataSet, Set<PartialPath> targetPaths)
      throws ContinuousQueryException {
    if (targetPaths.size() != queryDataSet.getDataTypes().size()) {
      throw new ContinuousQueryException(
          "the target paths generated by the pattern in into clause are duplicated. please change the pattern.");
    }
  }

  private void checkTargetPathDataType(GroupByTimePlan queryPlan, Set<PartialPath> targetPaths)
      throws MetadataException, ContinuousQueryException {
    TSDataType sourceDataType =
        TypeInferenceUtils.getAggrDataType(
            queryPlan.getAggregations().get(0), queryPlan.getDataTypes().get(0));
    for (PartialPath targetPath : targetPaths) {
      try {
        TSDataType targetPathDataType = IoTDB.schemaProcessor.getSeriesSchema(targetPath).getType();
        if (!sourceDataType.equals(targetPathDataType)) {
          throw new ContinuousQueryException(
              String.format(
                  "target path (%s) data type (%s) and source data type (%s) dose not match.",
                  targetPath.getFullPath(), targetPathDataType.name(), sourceDataType.name()));
        }
      } catch (PathNotExistException pathNotExistException) {
        if (!CONFIG.isAutoCreateSchemaEnabled()) {
          throw new ContinuousQueryException(
              String.format("target path (%s) dose not exist.", targetPath.getFullPath()));
        }

        // else ignored. we use the auto-create-schema feature.
      }
    }
  }

  private void tryExecuteQueryOnce(QueryDataSet queryDataSet) throws IOException {
    if (queryDataSet.hasNext()) {
      queryDataSet.next();
    }
  }
}
