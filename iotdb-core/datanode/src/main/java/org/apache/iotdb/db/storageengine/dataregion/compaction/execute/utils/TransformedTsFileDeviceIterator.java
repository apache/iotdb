/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils;

import java.io.IOException;
import java.util.function.Function;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.TsFileDeviceIterator;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.utils.Pair;

public class TransformedTsFileDeviceIterator extends TsFileDeviceIterator {

  protected Function<IDeviceID, IDeviceID> transformer;

  public TransformedTsFileDeviceIterator(TsFileSequenceReader reader, Function<IDeviceID, IDeviceID> transformer)
      throws IOException {
    super(reader);
    this.transformer = transformer;
  }

  public TransformedTsFileDeviceIterator(TsFileSequenceReader reader, String tableName, Function<IDeviceID, IDeviceID> transformer)
      throws IOException {
    super(reader, tableName);
    this.transformer = transformer;
  }

  @Override
  public Pair<IDeviceID, Boolean> next() {
    Pair<IDeviceID, Boolean> next = super.next();
    next.left = transformer.apply(next.left);
    return next;
  }
}
