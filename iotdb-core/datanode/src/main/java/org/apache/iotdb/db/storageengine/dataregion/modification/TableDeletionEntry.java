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
package org.apache.iotdb.db.storageengine.dataregion.modification;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.tsfile.read.common.TimeRange;

public class TableDeletionEntry extends ModEntry {
  private DeletionPredicate predicate;

  public TableDeletionEntry() {
    super(ModType.TABLE_DELETION);
  }

  public TableDeletionEntry(DeletionPredicate predicate, TimeRange timeRange) {
    this();
    this.predicate = predicate;
    this.timeRange = timeRange;
  }

  @Override
  public void serialize(OutputStream stream) throws IOException {
    super.serialize(stream);
    predicate.serialize(stream);
  }

  @Override
  public void deserialize(InputStream stream) throws IOException {
    super.deserialize(stream);
    predicate = new DeletionPredicate();
    predicate.deserialize(stream);
  }
}
