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
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.wal.WALManager;
import org.apache.iotdb.lsm.context.Context;
import org.apache.iotdb.lsm.context.DeleteContext;
import org.apache.iotdb.lsm.context.InsertContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecoverManager {
  private static final Logger logger = LoggerFactory.getLogger(RecoverManager.class);

  private WALManager walManager;

  public RecoverManager(WALManager walManager) {
    this.walManager = walManager;
  }

  public void recover(ITagInvertedIndex tagInvertedIndex) {
    logger.info("recover tagInvertedIndex");
    while (true) {
      Context context = walManager.read();
      switch (context.getType()) {
        case INSERT:
          tagInvertedIndex.addTags((InsertContext) context);
          break;
        case DELETE:
          tagInvertedIndex.removeTags((DeleteContext) context);
          break;
        default:
          return;
      }
    }
  }
}
