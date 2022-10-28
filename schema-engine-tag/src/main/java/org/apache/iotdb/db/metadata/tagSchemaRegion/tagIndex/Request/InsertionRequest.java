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
package org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.Request;

import org.apache.iotdb.lsm.context.requestcontext.RequestContext;
import org.apache.iotdb.lsm.request.IInsertionRequest;

import java.util.List;

/** Represents a insertion request */
public class InsertionRequest implements IInsertionRequest<String, Integer> {

  // tags
  List<String> keys;

  // int32 id
  int value;

  public InsertionRequest() {
    super();
  }

  public InsertionRequest(List<String> keys, int value) {
    super();
    this.keys = keys;
    this.value = value;
  }

  @Override
  public String getKey(RequestContext context) {
    return keys.get(context.getLevel() - 1);
  }

  @Override
  public List<String> getKeys() {
    return keys;
  }

  @Override
  public Integer getValue() {
    return value;
  }
}
