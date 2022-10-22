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
package org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.recover;

import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.ITagInvertedIndex;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.Request.DeletionRequest;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.Request.InsertionRequest;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.wal.WALManager;
import org.apache.iotdb.lsm.request.Request;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** for memory structure recovery */
public class RecoverManager {
  private static final Logger logger = LoggerFactory.getLogger(RecoverManager.class);

  private WALManager walManager;

  public RecoverManager(WALManager walManager) {
    this.walManager = walManager;
    walManager.setRecover(true);
  }

  /**
   * recover tagInvertedIndex
   *
   * @param tagInvertedIndex tag inverted index
   */
  public void recover(ITagInvertedIndex tagInvertedIndex) {
    logger.info("recover tagInvertedIndex");
    while (true) {
      Request request = walManager.read();
      if (request == null) {
        walManager.setRecover(false);
        return;
      }
      switch (request.getRequestType()) {
        case INSERT:
          tagInvertedIndex.addTags((InsertionRequest) request);
          break;
        case DELETE:
          tagInvertedIndex.removeTags((DeletionRequest) request);
          break;
        default:
          walManager.setRecover(false);
          return;
      }
    }
  }
}
