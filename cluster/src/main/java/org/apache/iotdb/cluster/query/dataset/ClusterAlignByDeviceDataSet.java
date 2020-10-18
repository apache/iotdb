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

package org.apache.iotdb.cluster.query.dataset;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.iotdb.cluster.metadata.CMManager;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.AlignByDevicePlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.AlignByDeviceDataSet;
import org.apache.iotdb.db.query.executor.IQueryRouter;
import org.apache.iotdb.db.service.IoTDB;

public class ClusterAlignByDeviceDataSet extends AlignByDeviceDataSet {


  public ClusterAlignByDeviceDataSet(
      AlignByDevicePlan alignByDevicePlan,
      QueryContext context,
      IQueryRouter queryRouter) {
    super(alignByDevicePlan, context, queryRouter);
  }

  @Override
  protected Set<String> getDeviceMeasurements(PartialPath device) throws IOException {
    try {
      List<PartialPath> matchedPaths = ((CMManager) IoTDB.metaManager).getMatchedPaths(device);
      Set<String> deviceMeasurements = new HashSet<>();
      for (PartialPath matchedPath : matchedPaths) {
        deviceMeasurements.add(matchedPath.getFullPath().substring(
            matchedPath.getFullPath().lastIndexOf(IoTDBConstant.PATH_SEPARATOR) + 1));
      }
      return deviceMeasurements;
    } catch (MetadataException e) {
      throw new IOException(e);
    }
  }
}
