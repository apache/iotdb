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
package org.apache.iotdb.consensus.ratis;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.SnapshotMeta;
import org.apache.iotdb.consensus.common.request.ByteBufferConsensusRequest;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.statemachine.IStateMachine;

import org.apache.ratis.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;

public class TestUtils {
  static class TestDataSet implements DataSet {
    private int number;

    public void setNumber(int number) {
      this.number = number;
    }

    public int getNumber() {
      return number;
    }
  }

  static class TestRequest {
    private final int cmd;

    public TestRequest(ByteBuffer buffer) {
      cmd = buffer.getInt();
    }

    public boolean isIncr() {
      return cmd == 1;
    }
  }

  static class IntegerCounter implements IStateMachine {
    private AtomicInteger integer;
    private final Logger logger = LoggerFactory.getLogger(IntegerCounter.class);

    @Override
    public void start() {
      integer = new AtomicInteger(0);
    }

    @Override
    public void stop() {}

    @Override
    public TSStatus write(IConsensusRequest IConsensusRequest) {
      ByteBufferConsensusRequest request = (ByteBufferConsensusRequest) IConsensusRequest;
      TestRequest testRequest = new TestRequest(request.getContent());
      if (testRequest.isIncr()) {
        integer.incrementAndGet();
      }
      return new TSStatus(200);
    }

    @Override
    public DataSet read(IConsensusRequest IConsensusRequest) {
      TestDataSet dataSet = new TestDataSet();
      dataSet.setNumber(integer.get());
      return dataSet;
    }

    @Override
    public void takeSnapshot(ByteBuffer metadata, File snapshotDir) {
      /**
       * When IStateMachine take the snapshot, it can directly use the metadata to name the snapshot
       * file. It's guaranteed that more up-to-date snapshot will have lexicographically larger
       * metadata.
       */
      String tempFilePath = snapshotDir + File.separator + ".tmp";
      String filePath = snapshotDir + File.separator + "snapshot." + new String(metadata.array());

      File tempFile = new File(tempFilePath);

      try {
        FileWriter writer = new FileWriter(tempFile);
        writer.write(String.valueOf(integer.get()));
        writer.close();
        tempFile.renameTo(new File(filePath));
      } catch (IOException e) {
        logger.error("take snapshot failed ", e);
      }
    }

    private Object[] getSortedPaths(File rootDir) {
      /**
       * When looking for the latest snapshot inside the directory, just list all filenames and sort
       * them.
       */
      ArrayList<Path> paths = new ArrayList<>();
      try {
        DirectoryStream<Path> stream = Files.newDirectoryStream(rootDir.toPath());
        for (Path path : stream) {
          paths.add(path);
        }
      } catch (IOException e) {
        logger.error("read directory failed ", e);
      }

      Object[] pathArray = paths.toArray();
      Arrays.sort(pathArray);
      return pathArray;
    }

    @Override
    public SnapshotMeta getLatestSnapshot(File snapshotDir) {
      Object[] pathArray = getSortedPaths(snapshotDir);
      if (pathArray.length == 0) {
        return null;
      }
      Path max = (Path) pathArray[pathArray.length - 1];

      String ordinal = max.getFileName().toString().split("\\.")[1];
      ByteBuffer metadata = ByteBuffer.wrap(ordinal.getBytes());
      return new SnapshotMeta(metadata, Collections.singletonList(max.toFile()));
    }

    @Override
    public void loadSnapshot(SnapshotMeta latest) {
      try {
        Scanner scanner = new Scanner(latest.getSnapshotFiles().get(0));
        int snapshotValue = Integer.parseInt(scanner.next());
        integer.set(snapshotValue);
        scanner.close();
      } catch (IOException e) {
        logger.error("read file failed ", e);
      }
    }

    @Override
    public void cleanUpOldSnapshots(File snapshotDir) {
      Object[] paths = getSortedPaths(snapshotDir);
      for (int i = 0; i < paths.length - 1; i++) {
        try {
          FileUtils.deleteFully((Path) paths[i]);
        } catch (IOException e) {
          logger.error("delete file failed ", e);
        }
      }
    }
  }
}
