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
package org.apache.iotdb.lsm.request;

import java.util.ArrayList;
import java.util.List;

public class QueryRequest<K> {
  boolean iterativeQuery;

  List<ISingleQueryRequest<K>> singleQueryRequests;

  public QueryRequest() {
    singleQueryRequests = new ArrayList<>();
  }

  public List<ISingleQueryRequest<K>> getSingleQueryRequests() {
    return singleQueryRequests;
  }

  public void setSingleQueryRequests(List<ISingleQueryRequest<K>> singleQueryRequests) {
    this.singleQueryRequests = singleQueryRequests;
  }

  public void add(ISingleQueryRequest<K> singleQueryRequest) {
    singleQueryRequests.add(singleQueryRequest);
  }

  public boolean isIterativeQuery() {
    return iterativeQuery;
  }

  public void setIterativeQuery(boolean iterativeQuery) {
    this.iterativeQuery = iterativeQuery;
  }
}
