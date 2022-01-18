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
 *
 */
package org.apache.iotdb.db.newsync.sender.recovery;

import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.newsync.sender.conf.SenderConf;
import org.apache.iotdb.db.newsync.sender.pipe.TsFilePipe;
import org.apache.iotdb.db.newsync.sender.pipe.TsFilePipeData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

public class TsFilePipeLogAnalyzer {
  private static final Logger logger = LoggerFactory.getLogger(TsFilePipeLogAnalyzer.class);

  private final String pipeDir;
  private final String pipeLogDir;

  private BlockingDeque<TsFilePipeData> pipeData;
  private long removeSerialNumber;

  public TsFilePipeLogAnalyzer(TsFilePipe pipe) {
    pipeDir = SenderConf.getPipeDir(pipe);
    pipeLogDir = new File(pipeDir, SenderConf.pipeLogDirName).getPath();
  }

  public BlockingDeque<TsFilePipeData> recover() throws IOException {
    pipeData = new LinkedBlockingDeque<>();

    deserializeRemoveSerialNumber();
    recoverHistoryData();

    return null;
  }

  private void deserializeRemoveSerialNumber() throws IOException {
    BufferedReader br =
        new BufferedReader(
            new FileReader(new File(pipeLogDir, SenderConf.removeSerialNumberLogName)));
    String readLine;
    while ((readLine = br.readLine()) != null) {
      removeSerialNumber = Long.parseLong(readLine);
    }
    br.close();
  }

  private void recoverHistoryData() {
    if (removeSerialNumber <= 0) {}
  }

  public boolean isCollectFinished() {
    return new File(pipeDir, SenderConf.pipeCollectFinishLockName).exists();
  }

  public static List<TsFilePipeData> parseFile(File file) throws IOException {
    List<TsFilePipeData> pipeData = new ArrayList<>();
    DataInputStream inputStream = new DataInputStream(new FileInputStream(file));
    try {
      while (true) {
        pipeData.add(TsFilePipeData.deserialize(inputStream));
      }
    } catch (EOFException e) {
      logger.info(String.format("Finish parsing pipeLog %s.", file.getPath()));
    } catch (IllegalPathException e) {
      logger.error(String.format("Parsing pipeLog %s error, because %s", file.getPath(), e));
      throw new IOException(e);
    }
    return pipeData;
  }
}
