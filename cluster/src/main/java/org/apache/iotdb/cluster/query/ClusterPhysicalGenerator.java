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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.logical.sys.LoadConfigurationOperator.LoadConfigurationOperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.LoadConfigurationPlan;
import org.apache.iotdb.db.qp.physical.sys.LoadConfigurationPlan.LoadConfigurationPlanType;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterPhysicalGenerator extends PhysicalGenerator {

  private static final Logger logger = LoggerFactory.getLogger(ClusterPhysicalGenerator.class);

  private MetaGroupMember metaGroupMember;

  ClusterPhysicalGenerator(MetaGroupMember metaGroupMember) {
    this.metaGroupMember = metaGroupMember;
  }

  @Override
  protected Pair<List<TSDataType>, List<TSDataType>> getSeriesTypes(List<String> paths,
      String aggregation) throws MetadataException {
    return metaGroupMember.getSeriesTypesByString(paths, aggregation);
  }

  @Override
  protected List<TSDataType> getSeriesTypes(List<Path> paths) throws MetadataException {
    List<String> pathStrs = new ArrayList<>(paths.size());
    for (Path path : paths) {
      pathStrs.add(path.getFullPath());
    }
    return metaGroupMember.getSeriesTypesByString(pathStrs, null).left;
  }

  @Override
  protected List<String> getMatchedTimeseries(String path) throws MetadataException {
    return metaGroupMember.getMatchedPaths(path);
  }

  @Override
  protected Set<String> getMatchedDevices(String path) throws MetadataException {
    return metaGroupMember.getMatchedDevices(path);
  }


  @Override
  protected PhysicalPlan generateLoadConfigurationPlan(LoadConfigurationOperatorType type)
      throws QueryProcessException {
    if (type == LoadConfigurationOperatorType.GLOBAL) {
      Properties[] properties = new Properties[2];
      properties[0] = new Properties();
      String iotdbEnginePropertiesUrl = IoTDBDescriptor.getInstance().getPropsUrl();
      try (InputStream inputStream = new FileInputStream(new File(iotdbEnginePropertiesUrl))) {
        properties[0].load(inputStream);
      } catch (IOException e) {
        logger.warn("Fail to find config file {}", iotdbEnginePropertiesUrl, e);
        throw new QueryProcessException("Fail to find iotdb-engine config file.");
      }
      String clusterPropertiesUrl = ClusterDescriptor.getInstance().getPropsUrl();
      properties[1] = new Properties();
      try (InputStream inputStream = new FileInputStream(new File(clusterPropertiesUrl))) {
        properties[1].load(inputStream);
      } catch (IOException e) {
        logger.warn("Fail to find config file {}", clusterPropertiesUrl, e);
        throw new QueryProcessException("Fail to find cluster config file.");
      }

      return new LoadConfigurationPlan(LoadConfigurationPlanType.GLOBAL, properties);
    } else {
      return new LoadConfigurationPlan(LoadConfigurationPlanType.LOCAL);
    }
  }
}
