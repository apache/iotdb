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

  /**
   * get all virtual storage group Processor
   * @return all virtual storage group Processor
   */
  public StorageGroupProcessor[] getAllVirutalStorageGroupProcessor(){
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
  @SuppressWarnings("java:S2445")
  // actually storageGroupMNode is a unique object on the mtree, synchronize it is reasonable
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
