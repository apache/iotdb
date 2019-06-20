/**
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

package org.apache.iotdb.db.writelog.recover;

import java.io.File;
import java.io.IOException;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.PrimitiveMemTable;
import org.apache.iotdb.db.engine.overflow.io.OverflowResource;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;
import org.apache.iotdb.tsfile.write.schema.FileSchema;

public class UnseqTsFileRecoverPerformer {

  private FileSchema fileSchema;
  private OverflowResource resource;


  public UnseqTsFileRecoverPerformer(OverflowResource resource, FileSchema fileSchema) {
    this.resource = resource;
    this.fileSchema = fileSchema;
  }

  public void recover() throws ProcessorException {
    IMemTable memTable = new PrimitiveMemTable();
    String logNodePrefix = resource.logNodePrefix();
    String insertFilePath = resource.getInsertFilePath();
    LogReplayer replayer = new LogReplayer(logNodePrefix, insertFilePath,
        resource.getModificationFile(), resource.getVersionController(), null,
        fileSchema, memTable);
    replayer.replayLogs();
    if (memTable.isEmpty()) {
      return;
    }
    try {
      resource.flush(fileSchema, memTable, logNodePrefix, 0, (a,b) -> {});
      resource.appendMetadatas();
      MultiFileLogNodeManager.getInstance().deleteNode(logNodePrefix + new File(insertFilePath).getName());
    } catch (IOException e) {
      throw new ProcessorException(e);
    }
  }

}
