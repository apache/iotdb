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
package org.apache.iotdb.lsm.manager;

import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.response.QueryResponse;
import org.apache.iotdb.lsm.context.requestcontext.QueryRequestContext;
import org.apache.iotdb.lsm.request.ISingleQueryRequest;
import org.apache.iotdb.lsm.request.QueryRequest;
import org.apache.iotdb.lsm.response.IQueryResponse;

public class QueryManager<T> {

  private MemQueryManager<T, ISingleQueryRequest> memQueryManager;

  private IDiskQueryManager diskQueryManager;

  private T rootMemNode;

  public <K, R extends IQueryResponse> R process(QueryRequest<K> queryRequest) {
    R memResponse = processMem(queryRequest);
    R diskResponse = processDisk(queryRequest);
    if (memResponse == null) {
      return diskResponse;
    } else if (diskResponse == null) {
      return memResponse;
    } else {
      if (queryRequest.isIterativeQuery()) {
        memResponse.addIterator(diskResponse.getIterator());
      } else {
        memResponse.or(diskResponse);
      }
      return memResponse;
    }
  }

  private <K, R extends IQueryResponse> R processMem(QueryRequest<K> queryRequest) {
    int i = 0;
    R queryResponse = null;
    for (ISingleQueryRequest<K> singleQueryRequest : queryRequest.getSingleQueryRequests()) {
      QueryRequestContext queryRequestContext = new QueryRequestContext();
      memQueryManager.process(rootMemNode, singleQueryRequest, queryRequestContext);
      R response = (R) queryRequestContext.getResponse();
      if (response == null) {
        return null;
      }
      if (i == 0) {
        queryResponse = response;
        i = 1;
      } else {
        queryResponse.and(response);
      }
    }
    return queryResponse;
  }

  private <K, R extends IQueryResponse> R processDisk(QueryRequest<K> queryRequest) {
    // enable flush
    if (diskQueryManager != null) {
      return diskQueryManager.process(queryRequest);
    }
    return (R) new QueryResponse();
  }

  public MemQueryManager<T, ISingleQueryRequest> getMemQueryManager() {
    return memQueryManager;
  }

  public void setMemQueryManager(MemQueryManager<T, ISingleQueryRequest> memQueryManager) {
    this.memQueryManager = memQueryManager;
  }

  public IDiskQueryManager getDiskQueryManager() {
    return diskQueryManager;
  }

  public void setDiskQueryManager(IDiskQueryManager diskQueryManager) {
    this.diskQueryManager = diskQueryManager;
  }

  public T getRootMemNode() {
    return rootMemNode;
  }

  public void setRootMemNode(T rootMemNode) {
    this.rootMemNode = rootMemNode;
  }
}
