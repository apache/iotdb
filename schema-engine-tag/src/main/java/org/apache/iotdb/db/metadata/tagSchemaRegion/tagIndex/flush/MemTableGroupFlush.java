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
package org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.flush;

import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.file.entry.TiFile;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemTable;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemTableGroup;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.response.FlushResponse;
import org.apache.iotdb.lsm.annotation.FlushProcessor;
import org.apache.iotdb.lsm.context.requestcontext.FlushRequestContext;
import org.apache.iotdb.lsm.levelProcess.FlushLevelProcessor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** flush for MemTableGroup */
@FlushProcessor(level = 0)
public class MemTableGroupFlush extends FlushLevelProcessor<MemTableGroup, MemTable> {

  @Override
  public List<MemTable> getChildren(
      MemTableGroup memNode, Object request, FlushRequestContext context) {
    FlushResponse flushResponse = new FlushResponse();
    Map<Integer, TiFile> tiFileMap = new HashMap<>();
    for (Map.Entry<Integer, MemTable> entry : memNode.getImmutableMemTables().entrySet()) {
      tiFileMap.put(entry.getKey(), new TiFile());
    }
    flushResponse.setValue(tiFileMap);
    flushResponse.setMemTableIndexMap(
        memNode.getImmutableMemTables().entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey)));
    context.setResponse(flushResponse);
    return (List<MemTable>) memNode.getImmutableMemTables().values();
  }

  @Override
  public void flush(MemTableGroup memNode, FlushRequestContext context) {}
}
