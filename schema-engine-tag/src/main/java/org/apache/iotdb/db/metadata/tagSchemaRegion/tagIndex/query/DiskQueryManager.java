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
package org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.query;

import org.apache.iotdb.db.metadata.tagSchemaRegion.config.TagSchemaRegionConstant;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.file.reader.DiskDeviceIDReader;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.response.QueryResponse;
import org.apache.iotdb.lsm.manager.IDiskQueryManager;
import org.apache.iotdb.lsm.request.ISingleQueryRequest;
import org.apache.iotdb.lsm.request.QueryRequest;
import org.apache.iotdb.lsm.response.IQueryResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Handle queries for tiFile */
public class DiskQueryManager implements IDiskQueryManager {
  private static final Logger logger = LoggerFactory.getLogger(DiskQueryManager.class);

  private String flushDirPath;

  private String flushFilePrefix;

  public DiskQueryManager(String flushDirPath, String flushFilePrefix) {
    this.flushDirPath = flushDirPath;
    this.flushFilePrefix = flushFilePrefix;
  }

  @Override
  public <K, R extends IQueryResponse> R process(QueryRequest<K> request) {
    if (request.isIterativeQuery()) {
      return getDeviceIDsIteratively(request);
    } else {
      return getAllDeviceIDs(request);
    }
  }

  private <K, R extends IQueryResponse> R getAllDeviceIDs(QueryRequest<K> request) {
    QueryResponse queryResponse = new QueryResponse();
    String[] tiFiles = getAllTiFiles();
    Map<String, String> tags = generateMap((QueryRequest<String>) request);
    DiskDeviceIDReader deviceIDReader = new DiskDeviceIDReader(tiFiles, tags, flushDirPath);
    queryResponse.setValue(deviceIDReader.getAllDeviceID());
    return (R) queryResponse;
  }

  private <K, R extends IQueryResponse> R getDeviceIDsIteratively(QueryRequest<K> request) {
    QueryResponse queryResponse = new QueryResponse();
    String[] tiFiles = getAllTiFiles();
    Map<String, String> tags = generateMap((QueryRequest<String>) request);
    DiskDeviceIDReader deviceIDReader = new DiskDeviceIDReader(tiFiles, tags, flushDirPath);
    queryResponse.addIterator(deviceIDReader);
    return (R) queryResponse;
  }

  private String[] getAllTiFiles() {
    File flushDir = new File(flushDirPath);
    return flushDir.list(
        (dir, name) ->
            name.startsWith(flushFilePrefix)
                && !name.endsWith(TagSchemaRegionConstant.TMP)
                && !name.contains(TagSchemaRegionConstant.DELETE));
  }

  private Map<String, String> generateMap(QueryRequest<String> request) {
    Map<String, String> map = new HashMap<>();
    for (ISingleQueryRequest<String> singleQueryRequest : request.getSingleQueryRequests()) {
      List<String> tag = singleQueryRequest.getKeys();
      map.put(tag.get(0), tag.get(1));
    }
    return map;
  }
}
