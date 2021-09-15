package org.apache.iotdb.db.service;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.settle.SettleLog;
import org.apache.iotdb.db.engine.settle.SettleTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.tools.settle.TsFileAndModSettleTool;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SettleService implements IService  {
  private AtomicInteger threadCnt = new AtomicInteger();    //线程数量，用作计数
  private ExecutorService settleThreadPool;
  private static AtomicInteger filesToBeSettledCount = new AtomicInteger();

  private static final Logger logger = LoggerFactory.getLogger(SettleService.class);

  public static SettleService getINSTANCE() {
    return InstanceHolder.INSTANCE;
  }

  public static class InstanceHolder {
    private static final SettleService INSTANCE = new SettleService();

    private InstanceHolder() {}
  }

  @Override
  public void start() {
    int settleThreadNum= IoTDBDescriptor.getInstance().getConfig().getSettleThreadNum();
    settleThreadPool =
        Executors.newFixedThreadPool(
            settleThreadNum, r -> new Thread(r, "SettleThread-" + threadCnt.getAndIncrement())); //创建整理线程池
    countSettleFiles(false);
    if(!SettleLog.createSettleLog()||filesToBeSettledCount.get()==0){
      stop();
      return;
    }
    settleAll();
  }

  @Override
  public void stop() {
    SettleLog.closeLogWriter();
    if (settleThreadPool != null) {
      settleThreadPool.shutdownNow();
      logger.info("Waiting for settle task pool to shut down");
      settleThreadPool = null;
      logger.info("Settle service stopped");
    }
  }

  @Override
  public ServiceType getID() {
    return ServiceType.SETTLE_SERVICE;
  }


  private void settleAll(){
    try {
      StorageEngine.getInstance().settleAll();
    } catch (StorageEngineException e) {
      logger.error("Cannot perform a global upgrade because", e);
    }
  }


  public static AtomicInteger getFilesToBeSettledCount() {
    return filesToBeSettledCount;
  }

  private static void countSettleFiles(boolean isRecover){  //Todo:bug
    if(isRecover){
      filesToBeSettledCount.addAndGet(TsFileAndModSettleTool.recoverSettleFileMap.size());
    }
    else{
      filesToBeSettledCount.addAndGet(StorageEngine.getInstance().countSettleFiles());
    }
  }

  public void submitSettleTask(SettleTask settleTask) {  //往该“整理TSFile文件”服务的线程池里提交该整理线程settleTask
    settleThreadPool.submit(settleTask);
  }

  public static void recoverSettle(){
    TsFileAndModSettleTool.findFilesToBeRecovered();
    if(TsFileAndModSettleTool.recoverSettleFileMap.size()==0){
      return;
    }
    countSettleFiles(true);
    for(Map.Entry<String,Integer> entry :TsFileAndModSettleTool.recoverSettleFileMap.entrySet()){
      String oldFilePath = entry.getKey();
      File oldFile = new File(oldFilePath);
      TsFileResource oldResource=new TsFileResource(oldFile);
      getINSTANCE().submitSettleTask(new SettleTask(oldResource));
    }
    TsFileAndModSettleTool.clearRecoverSettleFileMap();
  }
}
