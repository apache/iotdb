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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.fast.element;

import org.apache.iotdb.db.storageengine.dataregion.read.reader.common.MergeReaderPriority;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileID;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

@SuppressWarnings("squid:S1104")
public class FileElement {
  public TsFileResource resource;

  public boolean isSelected = false;
  public MergeReaderPriority priority;

  public FileElement(TsFileResource resource) {
    this.resource = resource;
    TsFileID tsFileID = resource.getTsFileID();
    this.priority =
        new MergeReaderPriority(tsFileID.timestamp, tsFileID.fileVersion, 0, resource.isSeq());
  }

  public MergeReaderPriority getPriority() {
    return this.priority;
  }
}
