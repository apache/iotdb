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

import org.apache.iotdb.lsm.context.requestcontext.RequestContext;

import java.util.List;

/**
 * Represents a request that can be processed by the lsm framework
 *
 * @param <K> The type of each layer key
 * @param <V> The type of value
 */
public interface IRequest<K, V> {

  /**
   * Get the key of a layer
   *
   * @param context request context
   * @return the key of the layer
   */
  K getKey(RequestContext context);

  /**
   * get all keys
   *
   * @return all keys
   */
  List<K> getKeys();

  /**
   * get the value
   *
   * @return value of the request
   */
  V getValue();

  /**
   * get request type
   *
   * @return request type
   */
  RequestType getRequestType();
}
