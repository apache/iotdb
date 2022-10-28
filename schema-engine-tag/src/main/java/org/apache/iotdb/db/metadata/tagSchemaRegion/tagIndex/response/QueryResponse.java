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
package org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.response;

import org.apache.iotdb.lsm.response.IResponse;

import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayList;
import java.util.List;

/** Represents a query response */
public class QueryResponse implements IResponse<RoaringBitmap> {

  // response value
  private RoaringBitmap roaringBitmap;

  // If an exception needs to be thrown during the processing of the request, this variable can be
  // used to accept the exception
  private List<Exception> exceptions;

  @Override
  public RoaringBitmap getValue() {
    return roaringBitmap;
  }

  @Override
  public void setValue(RoaringBitmap value) {
    this.roaringBitmap = value;
  }

  @Override
  public List<Exception> getExceptions() {
    return exceptions;
  }

  @Override
  public void setExceptions(List<Exception> exceptions) {
    this.exceptions = exceptions;
  }

  /**
   * If an exception needs to be thrown during the processing of the request, this method can be
   * used to accept the exception
   *
   * @param e Exception
   */
  @Override
  public void addException(Exception e) {
    if (exceptions == null) {
      exceptions = new ArrayList<>();
    }
    exceptions.add(e);
  }
}
