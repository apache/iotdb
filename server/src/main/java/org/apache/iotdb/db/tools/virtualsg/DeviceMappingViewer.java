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
package org.apache.iotdb.db.tools.virtualsg;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.storagegroup.virtualSg.HashVirtualPartitioner;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.PartialPath;

import java.util.Set;

/**
 * for DBA to view the mapping from device to virtual storage group ID usage: run this class with
 * arguments [system_schema_dir], if args are not given, use default in config
 */
public class DeviceMappingViewer {

  public static void main(String[] args) throws MetadataException {
    // has schema log dir
    if (args.length == 1) {
      IoTDBDescriptor.getInstance().getConfig().setSchemaDir(args[0]);
    }

    HashVirtualPartitioner partitioner = HashVirtualPartitioner.getInstance();
    IoTDBDescriptor.getInstance().getConfig().setEnableMTreeSnapshot(false);
    MManager mManager = MManager.getInstance();
    mManager.init();

    Set<PartialPath> partialPathSet = mManager.getMatchedDevices(new PartialPath("root.**"));

    if (partialPathSet.isEmpty() && args.length == 1) {
      System.out.println("no mlog in given system schema dir: " + args[0] + " please have a check");
    } else {
      System.out.println();
      System.out.println(
          "--------------------- mapping from device to virtual storage group ID ---------------------");
      System.out.println("Format is: device name -> virtual storage group ID");
      for (PartialPath partialPath : partialPathSet) {
        System.out.println(
            partialPath + " -> " + partitioner.deviceToVirtualStorageGroupId(partialPath));
      }
    }

    mManager.clear();
  }
}
