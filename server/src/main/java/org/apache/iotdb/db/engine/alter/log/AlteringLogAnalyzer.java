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

package org.apache.iotdb.db.engine.alter.log;

import org.apache.iotdb.db.engine.compaction.log.TsFileIdentifier;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** altering log analyzer */
public class AlteringLogAnalyzer {

  //  private static final Logger logger = LoggerFactory.getLogger(AlteringLogAnalyzer.class);

  private final File alterLogFile;
  private final List<Pair<String, Pair<TSEncoding, CompressionType>>> alterList =
      new ArrayList<>(4);
  private boolean clearBegin = false;
  private final Set<TsFileIdentifier> doneFiles = new HashSet<>(32);

  public AlteringLogAnalyzer(File alterLogFile) {
    this.alterLogFile = alterLogFile;
  }

  public void analyzer() throws IOException {

    try (BufferedReader bufferedReader = new BufferedReader(new FileReader(alterLogFile))) {

      String mark;
      while ((mark = bufferedReader.readLine()) != null) {
        if (AlteringLogger.FLAG_ALTER_PARAM_BEGIN.equals(mark)) {
          String fullPath = bufferedReader.readLine();
          if (fullPath == null) {
            throw new IOException("alter.log parse fail, fullPath is null");
          }
          TSEncoding encoding = TSEncoding.deserialize(Byte.parseByte(bufferedReader.readLine()));
          if (encoding == null) {
            throw new IOException("alter.log parse fail, encoding is null");
          }
          CompressionType compressionType =
              CompressionType.deserialize(Byte.parseByte(bufferedReader.readLine()));
          if (compressionType == null) {
            throw new IOException("alter.log parse fail, compressionType is null");
          }
          alterList.add(new Pair<>(fullPath, new Pair<>(encoding, compressionType)));
        } else if (AlteringLogger.FLAG_CLEAR_BEGIN.equals(mark)) {
          clearBegin = true;
        } else if (AlteringLogger.FLAG_DONE.equals(mark)) {
          String curLineStr = bufferedReader.readLine();
          doneFiles.add(TsFileIdentifier.getFileIdentifierFromInfoString(curLineStr));
        }
      }
    }
  }

  public List<Pair<String, Pair<TSEncoding, CompressionType>>> getAlterList() {
    return alterList;
  }

  public boolean isClearBegin() {
    return clearBegin;
  }

  public Set<TsFileIdentifier> getDoneFiles() {
    return doneFiles;
  }
}
