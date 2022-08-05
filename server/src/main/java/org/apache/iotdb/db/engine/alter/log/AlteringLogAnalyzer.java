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
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** altering log analyzer */
public class AlteringLogAnalyzer {

  //  private static final Logger logger = LoggerFactory.getLogger(AlteringLogAnalyzer.class);

  private final File alterLogFile;

  private String fullPath;

  private TSEncoding encoding;

  private CompressionType compressionType;

  public AlteringLogAnalyzer(File alterLogFile) {
    this.alterLogFile = alterLogFile;
  }

  public Map<Pair<Long, Boolean>, Set<TsFileIdentifier>> analyzer() throws IOException {

    final Map<Pair<Long, Boolean>, Set<TsFileIdentifier>> undoPartitionTsFiles = new HashMap<>();
    try (BufferedReader bufferedReader = new BufferedReader(new FileReader(alterLogFile))) {
      // file header
      fullPath = bufferedReader.readLine();
      if (fullPath == null) {
        throw new IOException("alter.log parse fail, fullPath is null");
      }
      encoding = TSEncoding.deserialize(Byte.parseByte(bufferedReader.readLine()));
      if (encoding == null) {
        throw new IOException("alter.log parse fail, encoding is null");
      }
      compressionType = CompressionType.deserialize(Byte.parseByte(bufferedReader.readLine()));
      if (compressionType == null) {
        throw new IOException("alter.log parse fail, compressionType is null");
      }
      // partition list
      String curPartition;
      while (true) {
        curPartition = bufferedReader.readLine();
        if (curPartition == null) {
          return undoPartitionTsFiles;
        }
        if (curPartition.equals(AlteringLogger.FLAG_TIME_PARTITIONS_HEADER_DONE)) {
          break;
        }
        Long partition = Long.parseLong(curPartition);
        undoPartitionTsFiles.put(new Pair<>(partition, true), new HashSet<>());
        undoPartitionTsFiles.put(new Pair<>(partition, false), new HashSet<>());
      }
      long readCurPartition;
      while (!undoPartitionTsFiles.isEmpty()) {
        // partition header
        // ftps partition isSeq
        String partitionHeaderStr = bufferedReader.readLine();
        if (partitionHeaderStr == null
            || !partitionHeaderStr.startsWith(AlteringLogger.FLAG_TIME_PARTITION_START)) {
          throw new IOException(
              "alter.log parse fail, line not start with FLAG_TIME_PARTITION_START");
        }
        String[] partitionHeaders = partitionHeaderStr.split(TsFileIdentifier.INFO_SEPARATOR);
        if (partitionHeaders.length != 3) {
          throw new IOException("alter.log parse fail, partitionHeaders error");
        }
        readCurPartition = Long.parseLong(partitionHeaders[1]);
        if (!AlteringLogger.FLAG_SEQ.equals(partitionHeaders[2])
            && !AlteringLogger.FLAG_UNSEQ.equals(partitionHeaders[2])) {
          throw new IOException("alter.log parse fail, partitionHeaders error");
        }
        Pair<Long, Boolean> key =
            new Pair<>(readCurPartition, AlteringLogger.FLAG_SEQ.equals(partitionHeaders[2]));
        Set<TsFileIdentifier> tsFileIdentifiers = undoPartitionTsFiles.get(key);
        String curLineStr;
        while (true) {
          // read tsfile list
          curLineStr = bufferedReader.readLine();
          if (curLineStr == null) {
            return undoPartitionTsFiles;
          }
          if (curLineStr.equals(AlteringLogger.FLAG_TIME_PARTITION_DONE)) {
            undoPartitionTsFiles.remove(key);
            break;
          }
          if (curLineStr.startsWith(AlteringLogger.FLAG_INIT_SELECTED_FILE)) {
            // add tsfile list
            tsFileIdentifiers.add(
                TsFileIdentifier.getFileIdentifierFromInfoString(
                    curLineStr.substring(
                        AlteringLogger.FLAG_INIT_SELECTED_FILE.length()
                            + TsFileIdentifier.INFO_SEPARATOR.length())));
          } else if (curLineStr.startsWith(AlteringLogger.FLAG_DONE)) {
            // remove done file
            remove(
                tsFileIdentifiers,
                TsFileIdentifier.getFileIdentifierFromInfoString(
                    curLineStr.substring(
                        AlteringLogger.FLAG_DONE.length()
                            + TsFileIdentifier.INFO_SEPARATOR.length())));
          } else {
            throw new IOException("alter.log parse fail, unknown line");
          }
        }
      }
    }
    return undoPartitionTsFiles;
  }

  private void remove(
      Set<TsFileIdentifier> tsFileIdentifiers, TsFileIdentifier endTsFileIdentifier) {

    TsFileIdentifier moveObj = null;
    for (TsFileIdentifier next : tsFileIdentifiers) {
      String filename = endTsFileIdentifier.getFilename();
      String oldFileName = next.getFilename();
      String preName =
          oldFileName.substring(0, oldFileName.length() - TsFileConstant.TSFILE_SUFFIX.length());
      if (filename.startsWith(preName)) {
        moveObj = next;
        break;
      }
    }
    tsFileIdentifiers.remove(moveObj);
  }

  public String getFullPath() {
    return fullPath;
  }

  public TSEncoding getEncoding() {
    return encoding;
  }

  public CompressionType getCompressionType() {
    return compressionType;
  }
}
