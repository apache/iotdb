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

package org.apache.iotdb.db.storageengine.dataregion.read.filescan;

import org.apache.iotdb.db.storageengine.dataregion.read.filescan.model.DeviceChunkMetaData;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.model.DeviceStartEndTime;
import org.apache.tsfile.file.metadata.IDeviceID;

/**
 * This interface is used to handle the scan of TSFile.
 * It will supply the interface for metadata check and chunkScan for one TsFile.
 */
public interface IFileScanHandle {

    /**
     * Get the metaData of devices in current TsFile.
     * MetaData includes the devicePath, startTime and endTime of specified devicePath.
     * @return the iterator of DeviceStartEndTime.
     */
    Iterable<DeviceStartEndTime> getDeviceStartEndTimeIterator();

    /**
     * Check whether specified timestamp in the devicePath is deleted in current TsFile.
     * @param deviceID the devicePath needs to be checked.
     * @param timestamp the timestamp for the devicePath.
     * @return if timestamp is deleted in mods file , return true, else return false.
     */
    boolean isDeviceTimeDeleted(IDeviceID deviceID, long timestamp);

    /**
     * Get the chunkMetaData of all devices in current TsFile.
     * MetaData includes the devicePath, measurementId and chunkMetaDataList of specified devicePath.
     * @return the iterator of DeviceChunkMetaData.
     */
    Iterable<DeviceChunkMetaData> getAllDeviceChunkMetaData();

    /**
     * If the TsFile of this handle is closed.
     */
    boolean isClosed();


    /**
     * If the TsFile of this handle is deleted.
     */
    boolean isDeleted();

    /**
     * Get the file path of target TsFile.
     */
    String getFilePath();
}
