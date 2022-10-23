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
package org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.metadata.tagSchemaRegion.config.TagSchemaConfig;
import org.apache.iotdb.db.metadata.tagSchemaRegion.config.TagSchemaDescriptor;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.Request.DeletionRequest;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.Request.InsertionRequest;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.Request.QueryRequest;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.deletion.MemChunkDeletion;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.deletion.MemChunkGroupDeletion;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.deletion.MemTableDeletion;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.deletion.MemTableGroupDeletion;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.insertion.MemChunkGroupInsertion;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.insertion.MemChunkInsertion;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.insertion.MemTableGroupInsertion;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.insertion.MemTableInsertion;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemTableGroup;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.query.MemChunkGroupQuery;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.query.MemChunkQuery;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.query.MemTableGroupQuery;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.query.MemTableQuery;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.wal.WALEntry;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.wal.WALManager;
import org.apache.iotdb.lsm.context.DeleteRequestContext;
import org.apache.iotdb.lsm.context.InsertRequestContext;
import org.apache.iotdb.lsm.context.QueryRequestContext;
import org.apache.iotdb.lsm.engine.LSMEngine;
import org.apache.iotdb.lsm.levelProcess.LevelProcessChain;
import org.apache.iotdb.lsm.manager.DeletionManager;
import org.apache.iotdb.lsm.manager.InsertionManager;
import org.apache.iotdb.lsm.manager.QueryManager;
import org.apache.iotdb.lsm.manager.RecoverManager;

import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TagInvertedIndex implements ITagInvertedIndex {

  private static final String WAL_FILE_NAME = "tag_inverted_index.log";

  private static final Logger logger = LoggerFactory.getLogger(TagInvertedIndex.class);

  private static final TagSchemaConfig tagSchemaConfig =
      TagSchemaDescriptor.getInstance().getTagSchemaConfig();

  LSMEngine<MemTableGroup> lsmEngine;

  public TagInvertedIndex(String schemaDirPath) {
    LevelProcessChain<MemTableGroup, InsertionRequest, InsertRequestContext>
        insertionLevelProcessChain = new LevelProcessChain<>();
    LevelProcessChain<MemTableGroup, DeletionRequest, DeleteRequestContext>
        deletionLevelProcessChain = new LevelProcessChain<>();
    LevelProcessChain<MemTableGroup, QueryRequest, QueryRequestContext> queryLevelProcessChain =
        new LevelProcessChain<>();
    insertionLevelProcessChain
        .nextLevel(new MemTableGroupInsertion())
        .nextLevel(new MemTableInsertion())
        .nextLevel(new MemChunkGroupInsertion())
        .nextLevel(new MemChunkInsertion());
    deletionLevelProcessChain
        .nextLevel(new MemTableGroupDeletion())
        .nextLevel(new MemTableDeletion())
        .nextLevel(new MemChunkGroupDeletion())
        .nextLevel(new MemChunkDeletion());
    queryLevelProcessChain
        .nextLevel(new MemTableGroupQuery())
        .nextLevel(new MemTableQuery())
        .nextLevel(new MemChunkGroupQuery())
        .nextLevel(new MemChunkQuery());
    try {
      WALManager walManager =
          new WALManager(
              schemaDirPath,
              WAL_FILE_NAME,
              tagSchemaConfig.getWalBufferSize(),
              new WALEntry(),
              false);
      InsertionManager<MemTableGroup, InsertionRequest> insertionManager =
          new InsertionManager<>(walManager);
      DeletionManager<MemTableGroup, DeletionRequest> deletionManager =
          new DeletionManager<>(walManager);
      QueryManager<MemTableGroup, QueryRequest> queryManager = new QueryManager<>();
      RecoverManager<LSMEngine<MemTableGroup>> recoverManager = new RecoverManager<>(walManager);
      insertionManager.setLevelProcessChain(insertionLevelProcessChain);
      deletionManager.setLevelProcessChain(deletionLevelProcessChain);
      queryManager.setLevelProcessChain(queryLevelProcessChain);
      lsmEngine = new LSMEngine<>();
      lsmEngine.setDeletionManager(deletionManager);
      lsmEngine.setInsertionManager(insertionManager);
      lsmEngine.setQueryManager(queryManager);
      lsmEngine.setWalManager(walManager);
      lsmEngine.setRecoverManager(recoverManager);
      lsmEngine.setRootMemNode(new MemTableGroup(tagSchemaConfig.getNumOfDeviceIdsInMemTable()));
      lsmEngine.recover();
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
  }

  @Override
  public synchronized void addTags(Map<String, String> tags, int id) {
    try {
      for (Map.Entry<String, String> tag : tags.entrySet()) {
        InsertionRequest insertionRequest =
            new InsertionRequest(generateKeys(tag.getKey(), tag.getValue()), id);
        lsmEngine.insert(insertionRequest);
      }
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
  }

  @Override
  public synchronized void removeTags(Map<String, String> tags, int id) {
    try {
      for (Map.Entry<String, String> tag : tags.entrySet()) {
        DeletionRequest deletionRequest =
            new DeletionRequest(generateKeys(tag.getKey(), tag.getValue()), id);
        lsmEngine.delete(deletionRequest);
      }
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
  }

  @Override
  public synchronized List<Integer> getMatchedIDs(Map<String, String> tags) {
    RoaringBitmap roaringBitmap = new RoaringBitmap();
    int i = 0;
    try {
      for (Map.Entry<String, String> tag : tags.entrySet()) {
        RoaringBitmap rb = getMatchedIDs(tag.getKey(), tag.getValue());
        if (rb == null) continue;
        else {
          if (i == 0) roaringBitmap = rb;
          else roaringBitmap = RoaringBitmap.and(roaringBitmap, rb);
          i++;
        }
      }
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
    return Arrays.stream(roaringBitmap.toArray()).boxed().collect(Collectors.toList());
  }

  private List<String> generateKeys(String tagKey, String tagValue) {
    List<String> keys = new ArrayList<>();
    keys.add(tagKey);
    keys.add(tagValue);
    return keys;
  }

  private RoaringBitmap getMatchedIDs(String tagKey, String tagValue) throws Exception {
    QueryRequest queryRequest = new QueryRequest(generateKeys(tagKey, tagValue));
    lsmEngine.query(queryRequest);
    return queryRequest.getResult();
  }

  @TestOnly
  public void clear() throws IOException {
    lsmEngine.clear();
  }
}
