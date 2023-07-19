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

/** Represents a flush request that can be processed by the lsm framework */
public interface IFlushRequest<K, V, T> extends IRequest<K, V> {

  T getMemNode();

  int getIndex();

  String getFlushDirPath();

  String getFlushFileName();

  String getFlushDeletionFileName();

  void setFlushDirPath(String dirPath);

  void setFlushFileName(String flushFileName);

  void setFlushDeleteFileName(String flushDeleteFileName);

  @Override
  default K getKey(RequestContext context) {
    return null;
  }

  @Override
  default List<K> getKeys() {
    return null;
  }

  @Override
  default V getValue() {
    return null;
  }

  @Override
  default RequestType getRequestType() {
    return RequestType.FLUSH;
  }
}
