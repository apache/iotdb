package org.apache.iotdb.db.service;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.exception.StartupException;

public class UpdateSevice implements IService {

  private static final UpdateSevice INSTANCE = new UpdateSevice();
  private static int updateThreadNum;
  private ExecutorService updateThreadPool;
  private AtomicInteger threadCnt = new AtomicInteger();


  private UpdateSevice() {
  }

  public static UpdateSevice getINSTANCE() {
    return INSTANCE;
  }

  @Override
  public void start() throws StartupException {
    updateThreadNum = IoTDBDescriptor.getInstance().getConfig().getUpdateThreadNum();
    if (updateThreadNum <= 0) {
      updateThreadNum = 1;
    }
    updateThreadPool = Executors.newFixedThreadPool(updateThreadNum,
        r -> new Thread(r, "UpdateThread-" + threadCnt.getAndIncrement()));

    for (StorageGroupProcessor storageGroupProcessor : StorageEngine.getInstance().getProcessorMap()
        .values()){

    }
  }

  @Override
  public void stop() {

  }

  @Override
  public ServiceType getID() {
    return ServiceType.UPDATE_SERVICE;
  }
}
