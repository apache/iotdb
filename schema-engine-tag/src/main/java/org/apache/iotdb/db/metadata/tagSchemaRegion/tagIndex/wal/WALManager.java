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
package org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.wal;

import org.apache.iotdb.lsm.context.Context;
import org.apache.iotdb.lsm.context.DeleteContext;
import org.apache.iotdb.lsm.context.InsertContext;
import org.apache.iotdb.lsm.wal.WALReader;
import org.apache.iotdb.lsm.wal.WALWriter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WALManager {
  private static final String WAL_FILE_NAME = "tagInvertedIndex.log";
  private static final int INSERT = 1;
  private static final int DELETE = 2;
  private final String schemaDirPath;
  private File walFile;
  private WALWriter walWriter;
  private WALReader walReader;

  public WALManager(String schemaDirPath) throws IOException {
    this.schemaDirPath = schemaDirPath;
    initFile(schemaDirPath);
    walWriter = new WALWriter(walFile, false);
    walReader = new WALReader(walFile, new WALEntry());
  }

  private void initFile(String schemaDirPath) throws IOException {
    File schemaDir = new File(schemaDirPath);
    schemaDir.mkdirs();
    walFile = new File(this.schemaDirPath, WAL_FILE_NAME);
    if (!walFile.exists()) {
      walFile.createNewFile();
    }
  }

  public synchronized void write(Context context) throws IOException {
    switch (context.getType()) {
      case INSERT:
        process((InsertContext) context);
        break;
      case DELETE:
        process((DeleteContext) context);
        break;
      default:
        break;
    }
  }

  public synchronized Context read() {
    if (walReader.hasNext()) {
      WALEntry walEntry = (WALEntry) walReader.next();
      if (walEntry.getType() == INSERT) {
        return generateInsertContext(walEntry);
      }
      if (walEntry.getType() == DELETE) {
        return generateDeleteContext(walEntry);
      }
    }
    return new Context();
  }

  private InsertContext generateInsertContext(WALEntry walEntry) {
    InsertContext insertContext = new InsertContext(walEntry.getDeviceID(), walEntry.getKeys());
    return insertContext;
  }

  private DeleteContext generateDeleteContext(WALEntry walEntry) {
    DeleteContext deleteContext = new DeleteContext(walEntry.getDeviceID(), walEntry.getKeys());
    return deleteContext;
  }

  private void process(InsertContext insertContext) throws IOException {
    List<Object> objects = insertContext.getKeys();
    List<String> keys = new ArrayList<>();
    for (Object o : objects) {
      keys.add((String) o);
    }
    WALEntry walEntry = new WALEntry(INSERT, keys, (Integer) insertContext.getValue());
    walWriter.write(walEntry);
  }

  private void process(DeleteContext deleteContext) throws IOException {
    List<Object> objects = deleteContext.getKeys();
    List<String> keys = new ArrayList<>();
    for (Object o : objects) {
      keys.add((String) o);
    }
    WALEntry walEntry = new WALEntry(DELETE, keys, (Integer) deleteContext.getValue());
    walWriter.write(walEntry);
  }

  public void close() throws IOException {
    walWriter.close();
    walReader.close();
  }
}
