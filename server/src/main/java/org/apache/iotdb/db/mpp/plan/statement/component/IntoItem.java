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
import org.apache.iotdb.db.mpp.plan.statement.StatementNode;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.SPACE;
import static org.apache.iotdb.commons.conf.IoTDBConstant.DOUBLE_COLONS;
import static org.apache.iotdb.commons.conf.IoTDBConstant.LEVELED_PATH_TEMPLATE_PATTERN;

public class IntoItem extends StatementNode {

  private final PartialPath intoDevice;
  private final List<String> intoMeasurements;
  private final boolean isAligned;

  public IntoItem(PartialPath intoDevice, List<String> intoMeasurements, boolean isAligned) {
    this.intoDevice = intoDevice;
    this.intoMeasurements = intoMeasurements;
    this.isAligned = isAligned;
  }

  public PartialPath getIntoDevice() {
    return intoDevice;
  }

  public List<String> getIntoMeasurements() {
    return intoMeasurements;
  }

  public boolean isAligned() {
    return isAligned;
  }

  public boolean isDeviceExistPlaceholder() {
    return intoDevice.containNode(DOUBLE_COLONS)
        || LEVELED_PATH_TEMPLATE_PATTERN.matcher(intoDevice.getFullPath()).find();
  }

  public boolean isMeasurementsExistPlaceholder() {
    for (String measurement : intoMeasurements) {
      if (measurement.equals(DOUBLE_COLONS)
          || LEVELED_PATH_TEMPLATE_PATTERN.matcher(measurement).find()) {
        return true;
      }
    }
    return false;
  }

  public List<PartialPath> getIntoPaths() {
    return intoMeasurements.stream().map(intoDevice::concatNode).collect(Collectors.toList());
  }

  public String toSQLString() {
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append(intoDevice);
    sqlBuilder.append('(');
    for (int i = 0; i < intoMeasurements.size(); i++) {
      sqlBuilder.append(intoMeasurements.get(i));
      if (i < intoMeasurements.size() - 1) {
        sqlBuilder.append(',').append(SPACE);
      }
    }
    sqlBuilder.append(')');
    return sqlBuilder.toString();
  }
}
