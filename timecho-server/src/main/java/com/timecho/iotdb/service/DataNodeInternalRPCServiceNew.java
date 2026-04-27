package com.timecho.iotdb.service;

import org.apache.iotdb.db.service.DataNodeInternalRPCService;
import org.apache.iotdb.mpp.rpc.thrift.IDataNodeRPCService.Processor;

public class DataNodeInternalRPCServiceNew extends DataNodeInternalRPCService {

  @Override
  public void initTProcessor() {
    impl.compareAndSet(null, new DataNodeInternalRPCServiceImplNew(dataNodeContext));
    initSyncedServiceImpl(null);
    processor = new Processor<>(impl.get());
  }

  private static class DataNodeInternalRPCServiceHolder {
    private static final DataNodeInternalRPCServiceNew INSTANCE =
        new DataNodeInternalRPCServiceNew();

    private DataNodeInternalRPCServiceHolder() {}
  }

  public static DataNodeInternalRPCServiceNew getInstance() {
    return DataNodeInternalRPCServiceHolder.INSTANCE;
  }
}
