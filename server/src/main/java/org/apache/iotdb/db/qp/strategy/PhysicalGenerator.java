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
package org.apache.iotdb.db.qp.strategy;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.sys.LoadConfigurationOperator.LoadConfigurationOperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.LoadConfigurationPlan;
import org.apache.iotdb.db.qp.physical.sys.LoadConfigurationPlan.LoadConfigurationPlanType;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.List;

/** Used to convert logical operator to physical plan */
public class PhysicalGenerator {

  public PhysicalPlan transformToPhysicalPlan(Operator operator) throws QueryProcessException {
    PhysicalPlan physicalPlan = operator.generatePhysicalPlan(this);
    physicalPlan.setDebug(operator.isDebug());
    return physicalPlan;
  }

  public PhysicalPlan generateLoadConfigurationPlan(LoadConfigurationOperatorType type)
      throws QueryProcessException {
    switch (type) {
      case GLOBAL:
        return new LoadConfigurationPlan(LoadConfigurationPlanType.GLOBAL);
      case LOCAL:
        return new LoadConfigurationPlan(LoadConfigurationPlanType.LOCAL);
      default:
        throw new QueryProcessException(
            String.format("Unrecognized load configuration operator type, %s", type.name()));
    }
  }

  public List<TSDataType> getSeriesTypes(List<PartialPath> paths) throws MetadataException {
    return SchemaUtils.getSeriesTypesByPaths(paths);
  }

  public List<PartialPath> groupVectorPaths(List<PartialPath> paths) throws MetadataException {
    return IoTDB.metaManager.groupVectorPaths(paths);
  }
}
