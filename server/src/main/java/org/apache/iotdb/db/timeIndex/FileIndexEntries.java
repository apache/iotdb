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
package org.apache.iotdb.db.timeIndex;

import java.util.Map;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;

/**
 * Time Index, like deviceIndex: [(deviceId, startTime, endTime, TsFilePath)] to accelerate query
 */
public class FileIndexEntries {

  private TimeIndexEntry[] indexEntries;
  private String tsFilePath;

  public TimeIndexEntry[] getIndexEntries() {
    return indexEntries;
  }

  public void setIndexEntries(TimeIndexEntry[] indexEntries) {
    this.indexEntries = indexEntries;
  }

  public String getTsFilePath() {
    return tsFilePath;
  }

  public void setTsFilePath(String tsFilePath) {
    this.tsFilePath = tsFilePath;
  }

  public static FileIndexEntries convertFromTsFileResource(TsFileResource resource)
      throws IllegalPathException {
    FileIndexEntries fileIndexEntries = new FileIndexEntries();
    TimeIndexEntry[] timeIndexEntries = new TimeIndexEntry[resource.getDeviceToIndexMap().size()];
    int i = 0;
    for (Map.Entry<String, Integer> entry : resource.getDeviceToIndexMap().entrySet()) {
      TimeIndexEntry timeIndexEntry = new TimeIndexEntry();
      timeIndexEntry.setAllElem(
          new PartialPath(entry.getKey()),
          resource.getStartTime(entry.getValue()),
          resource.getEndTime(entry.getValue()));
      timeIndexEntries[i++] = timeIndexEntry;
    }
    fileIndexEntries.setIndexEntries(timeIndexEntries);
    fileIndexEntries.setTsFilePath(resource.getTsFilePath());

    return fileIndexEntries;
  }
}