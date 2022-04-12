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

package org.apache.iotdb.db.service.thrift.impl;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngine;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.*;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataNodeManagementServiceImpl implements ManagementIService.Iface {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataNodeManagementServiceImpl.class);
  private SchemaEngine schemaEngine = SchemaEngine.getInstance();

  @Override
  public TSStatus createSchemaRegion(CreateSchemaRegionReq req) throws TException {
    TSStatus tsStatus;
    try {
      PartialPath storageGroupPartitionPath = new PartialPath(req.getStorageGroup());
      SchemaRegionId schemaRegionId = new SchemaRegionId(req.getRegionReplicaSet().getRegionId());
      schemaEngine.createSchemaRegion(storageGroupPartitionPath, schemaRegionId);
      tsStatus = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (IllegalPathException e1) {
      LOGGER.error(
          "Create Schema Region {} failed because path is illegal.", req.getStorageGroup());
      tsStatus = new TSStatus(TSStatusCode.PATH_ILLEGAL.getStatusCode());
      tsStatus.setMessage("Create Schema Region failed because storageGroup path is illegal.");
    } catch (MetadataException e2) {
      LOGGER.error(
          "Create Schema Region {} failed because {}", req.getStorageGroup(), e2.getMessage());
      tsStatus = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      tsStatus.setMessage(
          String.format("Create Schema Region failed because of %s", e2.getMessage()));
    }
    return tsStatus;
  }

  @Override
  public TSStatus createDataRegion(CreateDataRegionReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus createDataPartition(CreateDataPartitionReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus migrateSchemaRegion(MigrateSchemaRegionReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus migrateDataRegion(MigrateDataRegionReq req) throws TException {
    return null;
  }

  public void handleClientExit() {}

  // TODO: add Mpp interface
}
