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

import org.apache.tsfile.file.metadata.IDeviceID;

import java.io.IOException;

/** This interface is used to handle the scan of chunks in TSFile. */
public interface IChunkHandle {

  /** Check If there is more pages to be scanned in Chunk. */
  boolean hasNextPage() throws IOException;

  /** Move to next page */
  void nextPage() throws IOException;

  /** Skip the current page */
  void skipCurrentPage();

  /**
   * Get the statistics time of page in Chunk.
   *
   * @return start time and end time of page.
   */
  long[] getPageStatisticsTime();

  /**
   * Scan the data in the page and get the timestamp. It will cause disk IO if tsFile is not in
   * memory.
   *
   * @return the iterator of timestamp.
   */
  long[] getDataTime() throws IOException;

  IDeviceID getDeviceID();

  String getMeasurement();
}
