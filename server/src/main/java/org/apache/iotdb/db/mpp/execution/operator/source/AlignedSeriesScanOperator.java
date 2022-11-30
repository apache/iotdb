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
package org.apache.iotdb.db.mpp.execution.operator.source;

import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.util.HashSet;

public class AlignedSeriesScanOperator extends AbstractSeriesScanOperator {

  public AlignedSeriesScanOperator(
      PlanNodeId sourceId,
      AlignedPath seriesPath,
      OperatorContext context,
      Filter timeFilter,
      Filter valueFilter,
      boolean ascending) {
    super(
        sourceId,
        new AlignedSeriesScanUtil(
            seriesPath,
            new HashSet<>(seriesPath.getMeasurementList()),
            context.getInstanceContext(),
            timeFilter,
            valueFilter,
            ascending),
        context,
        // time + all value columns
        (1L + seriesPath.getMeasurementList().size())
            * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte());
  }
}
