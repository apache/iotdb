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

package org.apache.iotdb.db.mpp.plan.statement.component;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.statement.StatementNode;

import java.util.List;

import static org.apache.iotdb.db.mpp.plan.analyze.SelectIntoUtils.checkIsAllRawSeriesQuery;

/** This class maintains information of {@code INTO} clause. */
public class IntoComponent extends StatementNode {

  public static String PLACEHOLDER_MISMATCH_ERROR_MSG =
      "select into: the correspondence between the placeholder and the raw time series could not be established.";
  public static String FORBID_PLACEHOLDER_ERROR_MSG =
      "select into: placeholders can only be used in raw time series data queries.";
  public static String DEVICE_NUM_MISMATCH_ERROR_MSG =
      "select into: the number of source devices and the number of target devices should be the same.";
  public static String PATH_NUM_MISMATCH_ERROR_MSG =
      "select into: the number of source columns and the number of target paths should be the same.";
  public static String DUPLICATE_TARGET_PATH_ERROR_MSG =
      "select into: target paths in into clause should be different.";
  public static String DEVICE_ALIGNMENT_INCONSISTENT_ERROR_MSG =
      "select into: alignment property must be the same for the same device.";

  private final List<IntoItem> intoItems;

  public IntoComponent(List<IntoItem> intoItems) {
    this.intoItems = intoItems;
  }

  public boolean isDeviceExistPlaceholder() {
    for (IntoItem intoItem : intoItems) {
      if (intoItem.isDeviceExistPlaceholder()) {
        return true;
      }
    }
    return false;
  }

  public boolean isMeasurementsExistPlaceholder() {
    for (IntoItem intoItem : intoItems) {
      if (intoItem.isMeasurementsExistPlaceholder()) {
        return true;
      }
    }
    return false;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // used in ALIGN BY TIME
  /////////////////////////////////////////////////////////////////////////////////////////////////

  public void validate(List<Expression> sourceColumns) {
    boolean isAllRawSeriesQuery = checkIsAllRawSeriesQuery(sourceColumns);

    if (!isAllRawSeriesQuery) {
      if (isDeviceExistPlaceholder() || isMeasurementsExistPlaceholder()) {
        throw new SemanticException(FORBID_PLACEHOLDER_ERROR_MSG);
      }
    }

    if (isMeasurementsExistPlaceholder()) {
      for (IntoItem intoItem : intoItems) {
        if (intoItem.getIntoMeasurements().size() != 1) {
          throw new SemanticException(PLACEHOLDER_MISMATCH_ERROR_MSG);
        }
      }

      if (isDeviceExistPlaceholder()) {
        if (intoItems.size() != 1) {
          throw new SemanticException(PLACEHOLDER_MISMATCH_ERROR_MSG);
        }
      } else {
        if (intoItems.size() != 1 && intoItems.size() != sourceColumns.size()) {
          throw new SemanticException(PLACEHOLDER_MISMATCH_ERROR_MSG);
        }
      }
    } else {
      int intoPathsNum =
          intoItems.stream().mapToInt(item -> item.getIntoMeasurements().size()).sum();
      if (intoPathsNum != sourceColumns.size()) {
        throw new SemanticException(PATH_NUM_MISMATCH_ERROR_MSG);
      }
    }
  }

  public IntoPathIterator getIntoPathIterator() {
    return new IntoPathIterator(
        intoItems, isDeviceExistPlaceholder(), isMeasurementsExistPlaceholder());
  }

  public static class IntoPathIterator extends AbstractIntoIterator {

    public IntoPathIterator(
        List<IntoItem> intoItems,
        boolean isDeviceExistPlaceholder,
        boolean isMeasurementsExistPlaceholder) {
      super(intoItems, isDeviceExistPlaceholder, isMeasurementsExistPlaceholder);
    }

    public void next() {
      if (isMeasurementsExistPlaceholder) {
        if (!isDeviceExistPlaceholder && intoItems.size() > 1) {
          deviceIndex++;
        }
      } else {
        measurementIndex++;
        if (measurementIndex == intoItems.get(deviceIndex).getIntoMeasurements().size()) {
          deviceIndex++;
          measurementIndex = 0;
        }
      }
    }
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // used in ALIGN BY DEVICE
  /////////////////////////////////////////////////////////////////////////////////////////////////

  public void validate(List<PartialPath> sourceDevices, List<Expression> sourceColumns) {
    boolean isAllRawSeriesQuery = checkIsAllRawSeriesQuery(sourceColumns);

    if (!isAllRawSeriesQuery && isMeasurementsExistPlaceholder()) {
      throw new SemanticException(FORBID_PLACEHOLDER_ERROR_MSG);
    }

    if (isDeviceExistPlaceholder()) {
      if (intoItems.size() != 1) {
        throw new SemanticException(PLACEHOLDER_MISMATCH_ERROR_MSG);
      }
    } else {
      if (intoItems.size() != sourceDevices.size()) {
        throw new SemanticException(DEVICE_NUM_MISMATCH_ERROR_MSG);
      }
    }

    for (IntoItem intoItem : intoItems) {
      List<String> intoMeasurements = intoItem.getIntoMeasurements();
      if (intoItem.isMeasurementsExistPlaceholder()) {
        if (intoMeasurements.size() != 1) {
          throw new SemanticException(PLACEHOLDER_MISMATCH_ERROR_MSG);
        }
      } else {
        if (intoMeasurements.size() != sourceColumns.size()) {
          throw new SemanticException(PATH_NUM_MISMATCH_ERROR_MSG);
        }
      }
    }
  }

  public IntoDeviceMeasurementIterator getIntoDeviceMeasurementIterator() {
    return new IntoDeviceMeasurementIterator(
        intoItems, isDeviceExistPlaceholder(), isMeasurementsExistPlaceholder());
  }

  public static class IntoDeviceMeasurementIterator extends AbstractIntoIterator {

    public IntoDeviceMeasurementIterator(
        List<IntoItem> intoItems,
        boolean isDeviceExistPlaceholder,
        boolean isMeasurementsExistPlaceholder) {
      super(intoItems, isDeviceExistPlaceholder, isMeasurementsExistPlaceholder);
    }

    public void nextDevice() {
      if (!isDeviceExistPlaceholder) {
        deviceIndex++;
        measurementIndex = 0;
      }
    }

    public void nextMeasurement() {
      if (!intoItems.get(deviceIndex).isMeasurementsExistPlaceholder()) {
        measurementIndex++;
        if (measurementIndex == intoItems.get(deviceIndex).getIntoMeasurements().size()) {
          measurementIndex = 0;
        }
      }
    }
  }

  public abstract static class AbstractIntoIterator {

    protected final List<IntoItem> intoItems;

    protected final boolean isDeviceExistPlaceholder;
    protected final boolean isMeasurementsExistPlaceholder;

    protected int deviceIndex;
    protected int measurementIndex;

    protected AbstractIntoIterator(
        List<IntoItem> intoItems,
        boolean isDeviceExistPlaceholder,
        boolean isMeasurementsExistPlaceholder) {
      this.intoItems = intoItems;
      this.isDeviceExistPlaceholder = isDeviceExistPlaceholder;
      this.isMeasurementsExistPlaceholder = isMeasurementsExistPlaceholder;
      this.deviceIndex = 0;
      this.measurementIndex = 0;
    }

    public PartialPath getDeviceTemplate() {
      return intoItems.get(deviceIndex).getIntoDevice();
    }

    public String getMeasurementTemplate() {
      return intoItems.get(deviceIndex).getIntoMeasurements().get(measurementIndex);
    }

    public boolean isAlignedDevice() {
      return intoItems.get(deviceIndex).isAligned();
    }
  }

  public String toSQLString() {
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append("INTO ");
    for (int i = 0; i < intoItems.size(); i++) {
      sqlBuilder.append(intoItems.get(i).toSQLString());
      if (i < intoItems.size() - 1) {
        sqlBuilder.append(", ");
      }
    }
    return sqlBuilder.toString();
  }
}
