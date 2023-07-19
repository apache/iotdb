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
package org.apache.iotdb.lsm.response;

import java.util.Iterator;

/**
 * The response after the lsm framework processes a query request, supports one-time and iterative
 * retrieval of query results. If one-time retrieval results return value, iterative retrieval
 * results return an iterator
 *
 * @see org.apache.iotdb.lsm.request.QueryRequest
 * @param <T> the type of the result of the query
 * @param <A> The type of iteratively fetching records
 */
public interface IQueryResponse<T, A> extends IResponse<T> {

  /**
   * During the query process of the lsm framework, it is necessary to support the union of the
   * results of two independent queries
   */
  void or(IQueryResponse<T, A> queryResponse);

  /**
   * During the query process of the lsm framework, it is necessary to support the intersection of
   * the results of two independent queries
   */
  void and(IQueryResponse<T, A> queryResponse);

  /** Get an iterator from the query response, enabling iterative fetching of records */
  Iterator<A> getIterator();

  /**
   * In the process of processing the query request by the lsm framework, if the iterator needs to
   * be obtained in the user's query request, an iterator can be added.
   */
  void addIterator(Iterator<A> iterator);
}
