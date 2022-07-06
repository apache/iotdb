/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.metadata.schemaregion;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.db.exception.metadata.DataTypeMismatchException;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.utils.TypeInferenceUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.slf4j.Logger;

import java.io.File;
import java.util.Objects;

public class SchemaRegionUtils {

  public static void deleteSchemaRegionFolder(String schemaRegionDirPath, Logger logger)
      throws MetadataException {
    File schemaRegionDir = SystemFileFactory.INSTANCE.getFile(schemaRegionDirPath);
    File[] sgFiles = schemaRegionDir.listFiles();
    if (sgFiles == null) {
      throw new MetadataException(
          String.format("Can't get files in schema region dir %s", schemaRegionDirPath));
    }
    for (File file : sgFiles) {
      if (file.delete()) {
        logger.info("delete schema region folder {}", schemaRegionDir.getAbsolutePath());
      } else {
        logger.info("delete schema region folder {} failed.", schemaRegionDir.getAbsolutePath());
        throw new MetadataException(
            String.format(
                "Failed to delete schema region folder %s", schemaRegionDir.getAbsolutePath()));
      }
    }

    if (schemaRegionDir.delete()) {
      logger.info("delete schema region folder {}", schemaRegionDir.getAbsolutePath());
    } else {
      logger.info("delete schema region folder {} failed.", schemaRegionDir.getAbsolutePath());
      throw new MetadataException(
          String.format(
              "Failed to delete schema region folder %s", schemaRegionDir.getAbsolutePath()));
    }
    final File storageGroupDir = schemaRegionDir.getParentFile();
    if (Objects.requireNonNull(storageGroupDir.listFiles()).length == 0) {
      storageGroupDir.delete();
    }
  }

  public static void checkDataTypeMatch(InsertPlan plan, int loc, TSDataType dataType)
      throws MetadataException {
    TSDataType insertDataType;
    if (plan instanceof InsertRowPlan) {
      if (!((InsertRowPlan) plan).isNeedInferType()) {
        // only when InsertRowPlan's values is object[], we should check type
        insertDataType = getTypeInLoc(plan, loc);
      } else {
        insertDataType = dataType;
      }
    } else {
      insertDataType = getTypeInLoc(plan, loc);
    }
    if (dataType != insertDataType) {
      String measurement = plan.getMeasurements()[loc];
      String device = plan.getDevicePath().getFullPath();
      throw new DataTypeMismatchException(
          device,
          measurement,
          insertDataType,
          dataType,
          plan.getMinTime(),
          plan.getFirstValueOfIndex(loc));
    }
  }

  private static TSDataType getTypeInLoc(InsertPlan plan, int loc) throws MetadataException {
    TSDataType dataType;
    if (plan instanceof InsertRowPlan) {
      InsertRowPlan tPlan = (InsertRowPlan) plan;
      dataType =
          TypeInferenceUtils.getPredictedDataType(tPlan.getValues()[loc], tPlan.isNeedInferType());
    } else if (plan instanceof InsertTabletPlan) {
      dataType = (plan).getDataTypes()[loc];
    } else {
      throw new MetadataException(
          String.format(
              "Only support insert and insertTablet, plan is [%s]", plan.getOperatorType()));
    }
    return dataType;
  }
}
