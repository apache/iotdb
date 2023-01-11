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
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemTableGroup;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.query.DiskQueryManager;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.request.DeletionRequest;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.request.InsertionRequest;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.request.SingleQueryRequest;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.response.QueryResponse;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.wal.WALEntry;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.wal.WALManager;
import org.apache.iotdb.lsm.engine.LSMEngine;
import org.apache.iotdb.lsm.engine.LSMEngineBuilder;
import org.apache.iotdb.lsm.request.QueryRequest;

import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** tag inverted index, tag is <tagkey,tagValue> and id is int32 auto increment id */
public class TagInvertedIndex implements ITagInvertedIndex {

  // This file records the wal log
  private static final String WAL_FILE_PREFIX = "tag_inverted_index_wal";

  private static final String FLUSH_FILE_PREFIX = "tag_inverted_index_flush";

  private static final String WAL_DIR_PATH = "wal";

  private static final String FLUSH_DIR_PATH = "flush";

  private static final Logger logger = LoggerFactory.getLogger(TagInvertedIndex.class);

  // Manage configuration information of tag schema region
  private static final TagSchemaConfig tagSchemaConfig =
      TagSchemaDescriptor.getInstance().getTagSchemaConfig();

  // Directly use the lsm engine that comes with the lsm framework to implement the tag inverted
  // index
  LSMEngine<MemTableGroup> lsmEngine;

  private String schemaDirPath;

  /**
   * initialization method
   *
   * @param schemaDirPath schema dirPath
   */
  public TagInvertedIndex(String schemaDirPath) {
    this.schemaDirPath = schemaDirPath;
    try {
      WALManager walManager =
          new WALManager(
              schemaDirPath + File.separator + WAL_DIR_PATH, WAL_FILE_PREFIX, new WALEntry());
      // root memory node, used to manage working and immutableMemTables
      MemTableGroup memTableGroup =
          new MemTableGroup(
              tagSchemaConfig.getNumOfDeviceIdsInMemTable(),
              tagSchemaConfig.getNumOfImmutableMemTable(),
              tagSchemaConfig.getMaxChunkSize(),
              tagSchemaConfig.isEnableFlush());

      DiskQueryManager diskQueryManager =
          new DiskQueryManager(schemaDirPath + File.separator + FLUSH_DIR_PATH, FLUSH_FILE_PREFIX);
      // build lsm engine
      lsmEngine =
          new LSMEngineBuilder<MemTableGroup>()
              .buildRootMemNode(memTableGroup)
              .buildLSMManagers(
                  "org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex",
                  walManager,
                  memTableGroup,
                  schemaDirPath + File.separator + FLUSH_DIR_PATH,
                  FLUSH_FILE_PREFIX,
                  diskQueryManager,
                  tagSchemaConfig.isEnableFlush())
              .build();

      // recover the lsm engine
      lsmEngine.recover();
    } catch (IOException e) {
      logger.info("TagInvertedIndex initialization failed");
      logger.error(e.getMessage());
    }
  }

  /**
   * insert tags and device id
   *
   * @param tags tags like: <tagKey,tagValue>
   * @param id INT32 device id
   */
  @Override
  public synchronized void addTags(Map<String, String> tags, int id) {
    for (Map.Entry<String, String> tag : tags.entrySet()) {
      InsertionRequest insertionRequest =
          new InsertionRequest(generateKeys(tag.getKey(), tag.getValue()), id);
      lsmEngine.insert(insertionRequest);
    }
  }

  /**
   * delete tags and id using delete request context
   *
   * @param tags tags like: <tagKey,tagValue>
   * @param id INT32 device id
   */
  @Override
  public synchronized void removeTags(Map<String, String> tags, int id) {
    for (Map.Entry<String, String> tag : tags.entrySet()) {
      DeletionRequest deletionRequest =
          new DeletionRequest(generateKeys(tag.getKey(), tag.getValue()), id);
      deletionRequest.setFlushFilePrefix(FLUSH_FILE_PREFIX);
      deletionRequest.setFlushDirPath(schemaDirPath + File.separator + FLUSH_DIR_PATH);
      lsmEngine.delete(deletionRequest);
    }
  }

  /**
   * get all matching device ids
   *
   * @param tags tags like: <tagKey,tagValue>
   * @return ids
   */
  @Override
  public synchronized List<Integer> getAllMatchedIDs(Map<String, String> tags) {
    QueryRequest<String> queryRequest = generateQueryRequest(tags);
    queryRequest.setIterativeQuery(false);
    QueryResponse queryResponse = lsmEngine.query(queryRequest);
    if (queryResponse == null) {
      return new ArrayList<>();
    }
    RoaringBitmap roaringBitmap = queryResponse.getValue();
    return Arrays.stream(roaringBitmap.toArray()).boxed().collect(Collectors.toList());
  }

  /**
   * get matching device ids iteratively
   *
   * @param tags tags like: <tagKey,tagValue>
   * @return a device id iterator
   */
  @Override
  public synchronized Iterator<Integer> getMatchedIDsIteratively(Map<String, String> tags) {
    QueryRequest<String> queryRequest = generateQueryRequest(tags);
    queryRequest.setIterativeQuery(true);
    QueryResponse queryResponse = lsmEngine.query(queryRequest);
    return queryResponse.getIterator();
  }

  /**
   * Generate the keys in the request
   *
   * @param tagKey tag key
   * @param tagValue tag value
   * @return keys
   */
  private List<String> generateKeys(String tagKey, String tagValue) {
    List<String> keys = new ArrayList<>();
    keys.add(tagKey);
    keys.add(tagValue);
    return keys;
  }

  private QueryRequest<String> generateQueryRequest(Map<String, String> tags) {
    QueryRequest<String> queryRequest = new QueryRequest<>();
    for (Map.Entry<String, String> entry : tags.entrySet()) {
      SingleQueryRequest singleQueryRequest =
          new SingleQueryRequest(generateKeys(entry.getKey(), entry.getValue()));
      queryRequest.add(singleQueryRequest);
    }
    return queryRequest;
  }

  /**
   * Close all open resources
   *
   * @throws IOException
   */
  @Override
  @TestOnly
  public void clear() throws IOException {
    lsmEngine.clear();
  }
}
