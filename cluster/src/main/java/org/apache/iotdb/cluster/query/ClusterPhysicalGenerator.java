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

package org.apache.iotdb.cluster.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;

public class ClusterPhysicalGenerator extends PhysicalGenerator {

  private MetaGroupMember metaGroupMember;

  ClusterPhysicalGenerator(MetaGroupMember metaGroupMember) {
    this.metaGroupMember = metaGroupMember;
  }

  @Override
  protected List<TSDataType> getSeriesTypes(List<String> paths,
      String aggregation) throws MetadataException {
    return metaGroupMember.getSeriesTypesByString(paths, aggregation);
  }

  @Override
  protected List<TSDataType> getSeriesTypes(List<Path> paths) throws MetadataException {
    List<String> pathStrs = new ArrayList<>(paths.size());
    for (Path path : paths) {
      pathStrs.add(path.getFullPath());
    }
    return metaGroupMember.getSeriesTypesByString(pathStrs, null);
  }

  @Override
  protected List<String> getMatchedTimeseries(String path) throws MetadataException {
    return metaGroupMember.getMatchedPaths(path);
  }

  @Override
  protected Set<String> getMatchedDevices(String path) throws MetadataException {
    return metaGroupMember.getMatchedDevices(path);
  }
}
