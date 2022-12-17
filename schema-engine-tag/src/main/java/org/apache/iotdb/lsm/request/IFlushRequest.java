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
public class IFlushRequest<K, V> implements IRequest<K, V> {
  private String flushDirPath;
  private String flushFileName;

  public int getIndex() {
    return index;
  }

  public void setIndex(int index) {
    this.index = index;
  }

  private int index;

  public IFlushRequest(int index) {
    this.index = index;
  }

  public String getFlushDirPath() {
    return flushDirPath;
  }

  public void setFlushDirPath(String flushDirPath) {
    this.flushDirPath = flushDirPath;
  }

  public String getFlushFileName() {
    return flushFileName;
  }

  public void setFlushFileName(String flushFileName) {
    this.flushFileName = flushFileName;
  }

  @Override
  public K getKey(RequestContext context) {
    return null;
  }

  @Override
  public List<K> getKeys() {
    return null;
  }

  @Override
  public V getValue() {
    return null;
  }

  @Override
  public RequestType getRequestType() {
    return RequestType.FLUSH;
  }
}
