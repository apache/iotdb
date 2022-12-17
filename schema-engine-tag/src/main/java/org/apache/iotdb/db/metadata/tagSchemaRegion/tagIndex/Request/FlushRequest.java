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

import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemTable;
import org.apache.iotdb.lsm.request.IFlushRequest;

public class FlushRequest extends IFlushRequest {

  private Integer id;

  private MemTable memTable;

  public FlushRequest(Integer id, MemTable memTable) {
    this.id = id;
    this.memTable = memTable;
  }

  public Integer getId() {
    return id;
  }

  public MemTable getMemTable() {
    return memTable;
  }

  public void setId(Integer id) {
    this.id = id;
  }

  public void setMemTable(MemTable memTable) {
    this.memTable = memTable;
  }
}
