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
package org.apache.iotdb.itbase.runtime;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;

/** This class is used to handle multi requests and gather their returned values. */
public abstract class RequestDelegate<T> {

  private final List<String> endpoints;
  private final List<Callable<T>> requests = new ArrayList<>();

  /**
   * Initialize a RequestDelegate instance with a list of endpoints.
   *
   * @param endpoints the list of endpoints.
   */
  public RequestDelegate(List<String> endpoints) {
    this.endpoints = endpoints;
  }

  /**
   * Add a request to this RequestDelegate. The request sequence should correspond to endpoints.
   *
   * @param request the request to add.
   */
  public final void addRequest(Callable<T> request) {
    requests.add(request);
  }

  /**
   * Do the requests which have been added, and return a list of their return values.
   *
   * @return the return values of all the request added in order.
   * @throws SQLException if any error happens during requesting.
   */
  public abstract List<T> requestAll() throws SQLException;

  /**
   * Do the requests which have been added, and then comparing their return values. If all the
   * values are equal, one value will be returned. Otherwise an {@link SQLException} will be thrown.
   *
   * @return the return value of one request, as all requests return the same one.
   * @throws SQLException if any error happens during requesting.
   * @throws InconsistentDataException if the return values of requests are not equal.
   */
  public final T requestAllAndCompare() throws SQLException {
    List<T> results = requestAll();
    T data = results.get(0);
    for (int i = 1; i < results.size(); i++) {
      T anotherData = results.get(i);
      if (!Objects.equals(data, anotherData)) {
        throw new InconsistentDataException(results, endpoints);
      }
    }
    return data;
  }

  protected List<String> getEndpoints() {
    return endpoints;
  }

  protected List<Callable<T>> getRequests() {
    return requests;
  }
}
