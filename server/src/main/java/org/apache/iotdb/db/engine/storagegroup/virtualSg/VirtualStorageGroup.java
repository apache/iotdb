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

public class VirtualStorageGroup {

  private static final Logger logger = LoggerFactory.getLogger(VirtualStorageGroup.class);

  VirtualPartitioner partitioner = HashVirtualPartitioner.getInstance();

  StorageGroupProcessor[] virtualPartition;


  public StorageGroupProcessor[] getAllPartition(){
    return virtualPartition;
  }

  public VirtualStorageGroup(){
    virtualPartition = new StorageGroupProcessor[partitioner.getPartitionCount()];
  }

  public void forceCloseAllWorkingTsFileProcessors() throws TsFileProcessorException {
    for(StorageGroupProcessor storageGroupProcessor : virtualPartition){
      if(storageGroupProcessor != null){
        storageGroupProcessor.forceCloseAllWorkingTsFileProcessors();
      }
    }
  }

  public void syncCloseAllWorkingTsFileProcessors(){
    for(StorageGroupProcessor storageGroupProcessor : virtualPartition){
      if(storageGroupProcessor != null){
        storageGroupProcessor.syncCloseAllWorkingTsFileProcessors();
      }
    }
  }

  public void checkTTL(){
    for(StorageGroupProcessor storageGroupProcessor : virtualPartition){
      if(storageGroupProcessor != null){
        storageGroupProcessor.checkFilesTTL();
      }
    }
  }

  /**
   *
   * @param partialPath device path
   * @return virtual storage group processor
   */
  public StorageGroupProcessor getProcessor(PartialPath partialPath, StorageGroupMNode storageGroupMNode)
      throws StorageGroupProcessorException, StorageEngineException {
    int loc = partitioner.deviceToStorageGroup(partialPath);

    StorageGroupProcessor processor = virtualPartition[loc];
    if (processor == null) {
      // if finish recover
      if (StorageEngine.getInstance().isAllSgReady()) {
        synchronized (storageGroupMNode) {
          processor = virtualPartition[loc];
          if (processor == null) {
            processor = StorageEngine.getInstance()
                .buildNewStorageGroupProcessor(partialPath, storageGroupMNode, String.valueOf(loc));
            virtualPartition[loc] = processor;
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

  public void recover(StorageGroupMNode storageGroupMNode) throws StorageGroupProcessorException {
    for (int i = 0; i < partitioner.getPartitionCount(); i++) {
      StorageGroupProcessor processor = StorageEngine.getInstance()
          .buildNewStorageGroupProcessor(storageGroupMNode.getPartialPath(), storageGroupMNode, String.valueOf(i));
      virtualPartition[i] = processor;
    }
  }
}
