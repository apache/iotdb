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
package org.apache.iotdb.db.sync.pipedata.load;

import org.apache.iotdb.commons.exception.sync.PipeDataLoadException;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.mpp.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.ClusterSchemaFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.ISchemaFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.StandalonePartitionFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.StandaloneSchemaFetcher;

/**
 * This interface is used to load files, including tsFile, syncTask, schema, modsFile and
 * deletePlan.
 */
public interface ILoader {

  IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  IPartitionFetcher PARTITION_FETCHER =
      config.isClusterMode()
          ? ClusterPartitionFetcher.getInstance()
          : StandalonePartitionFetcher.getInstance();

  ISchemaFetcher SCHEMA_FETCHER =
      config.isClusterMode()
          ? ClusterSchemaFetcher.getInstance()
          : StandaloneSchemaFetcher.getInstance();

  void load() throws PipeDataLoadException;
}
