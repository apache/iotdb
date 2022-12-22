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

import org.apache.iotdb.lsm.response.BaseResponse;
import org.apache.iotdb.lsm.response.IQueryResponse;

import org.roaringbitmap.RoaringBitmap;

public class QueryResponse extends BaseResponse<RoaringBitmap>
    implements IQueryResponse<RoaringBitmap> {

  public QueryResponse() {
    super(new RoaringBitmap());
  }

  @Override
  public void or(IQueryResponse<RoaringBitmap> queryResponse) {
    getValue().or(queryResponse.getValue());
  }

  @Override
  public void and(IQueryResponse<RoaringBitmap> queryResponse) {
    getValue().and(queryResponse.getValue());
  }
}
