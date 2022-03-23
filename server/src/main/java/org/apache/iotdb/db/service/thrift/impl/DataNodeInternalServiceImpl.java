package org.apache.iotdb.db.service.thrift.impl;

import org.apache.iotdb.service.rpc.thrift.*;

import org.apache.thrift.TException;

public class DataNodeInternalServiceImpl implements ManagementIService.Iface {
  @Override
  public TSStatus createSchemaRegion(CreateSchemaRegionReq req) throws TException {
    return null;
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
