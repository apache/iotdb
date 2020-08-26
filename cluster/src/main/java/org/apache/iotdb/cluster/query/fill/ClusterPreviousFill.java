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

package org.apache.iotdb.cluster.query.fill;

import java.util.Set;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.executor.fill.PreviousFill;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterPreviousFill extends PreviousFill {

  private static final Logger logger = LoggerFactory.getLogger(ClusterPreviousFill.class);
  private MetaGroupMember metaGroupMember;
  private TimeValuePair fillResult;

  ClusterPreviousFill(PreviousFill fill, MetaGroupMember metaGroupMember) {
    super(fill.getDataType(), fill.getQueryTime(), fill.getBeforeRange());
    this.metaGroupMember = metaGroupMember;
  }

  @Override
  public void configureFill(Path path, TSDataType dataType, long queryTime, Set<String> deviceMeasurements,
      QueryContext context)  {
    try {
      fillResult = metaGroupMember.performPreviousFill(path, dataType, queryTime, getBeforeRange(),
          deviceMeasurements, context);
    } catch (StorageEngineException e) {
      logger.error("Failed to configure previous fill for Path {}", path, e);
    }
  }

  @Override
  public TimeValuePair getFillResult() {
    return fillResult;
  }
}
