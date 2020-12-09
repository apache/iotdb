package org.apache.iotdb.db.engine.storagegroup.virtualSg;

import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.StorageGroupProcessorException;
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.rpc.TSStatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VirtualStorageGroupManager {

  private static final Logger logger = LoggerFactory.getLogger(VirtualStorageGroupManager.class);

  /**
   * virtual storage group partitioner
   */
  VirtualPartitioner partitioner = HashVirtualPartitioner.getInstance();

  /**
   * all virtual storage group processor
   */
  StorageGroupProcessor[] virtualStorageGroupProcessor;


  public StorageGroupProcessor[] getAllPartition(){
    return virtualStorageGroupProcessor;
  }

  public VirtualStorageGroupManager(){
    virtualStorageGroupProcessor = new StorageGroupProcessor[partitioner.getPartitionCount()];
  }

  /**
   * push forceCloseAllWorkingTsFileProcessors down to all sg
   */
  public void forceCloseAllWorkingTsFileProcessors() throws TsFileProcessorException {
    for(StorageGroupProcessor storageGroupProcessor : virtualStorageGroupProcessor){
      if(storageGroupProcessor != null){
        storageGroupProcessor.forceCloseAllWorkingTsFileProcessors();
      }
    }
  }

  /**
   * push syncCloseAllWorkingTsFileProcessors down to all sg
   */
  public void syncCloseAllWorkingTsFileProcessors(){
    for(StorageGroupProcessor storageGroupProcessor : virtualStorageGroupProcessor){
      if(storageGroupProcessor != null){
        storageGroupProcessor.syncCloseAllWorkingTsFileProcessors();
      }
    }
  }

  /**
   * push check ttl down to all sg
   */
  public void checkTTL(){
    for(StorageGroupProcessor storageGroupProcessor : virtualStorageGroupProcessor){
      if(storageGroupProcessor != null){
        storageGroupProcessor.checkFilesTTL();
      }
    }
  }

  /**
   * get processor from device id
   * @param partialPath device path
   * @return virtual storage group processor
   */
  public StorageGroupProcessor getProcessor(PartialPath partialPath, StorageGroupMNode storageGroupMNode)
      throws StorageGroupProcessorException, StorageEngineException {
    int loc = partitioner.deviceToStorageGroup(partialPath);

    StorageGroupProcessor processor = virtualStorageGroupProcessor[loc];
    if (processor == null) {
      // if finish recover
      if (StorageEngine.getInstance().isAllSgReady()) {
        synchronized (storageGroupMNode) {
          processor = virtualStorageGroupProcessor[loc];
          if (processor == null) {
            processor = StorageEngine.getInstance()
                .buildNewStorageGroupProcessor(partialPath, storageGroupMNode, String.valueOf(loc));
            virtualStorageGroupProcessor[loc] = processor;
          }
        }
      } else {
        // not finished recover, refuse the request
        throw new StorageEngineException(
            "the sg " + partialPath + " may not ready now, please wait and retry later",
            TSStatusCode.STORAGE_GROUP_NOT_READY.getStatusCode());
      }
    }

    return processor;
  }

  /**
   * recover
   * @param storageGroupMNode logical sg mnode
   */
  public void recover(StorageGroupMNode storageGroupMNode) throws StorageGroupProcessorException {
    for (int i = 0; i < partitioner.getPartitionCount(); i++) {
      StorageGroupProcessor processor = StorageEngine.getInstance()
          .buildNewStorageGroupProcessor(storageGroupMNode.getPartialPath(), storageGroupMNode, String.valueOf(i));
      virtualStorageGroupProcessor[i] = processor;
    }
  }
}
