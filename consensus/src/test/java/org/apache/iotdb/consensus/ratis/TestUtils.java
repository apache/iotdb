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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.ByteBufferConsensusRequest;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;

public class TestUtils {
  public static class TestDataSet implements DataSet {
    private int number;

    public void setNumber(int number) {
      this.number = number;
    }

    public int getNumber() {
      return number;
    }
  }

  public static class TestRequest implements IConsensusRequest {
    private final int cmd;

    public TestRequest(ByteBuffer buffer) {
      cmd = buffer.getInt();
    }

    public boolean isIncr() {
      return cmd == 1;
    }

    @Override
    public ByteBuffer serializeToByteBuffer() {
      ByteBuffer buffer = ByteBuffer.allocate(4).putInt(cmd);
      buffer.flip();
      return buffer;
    }
  }

  public static class IntegerCounter implements IStateMachine, IStateMachine.EventApi {
    private AtomicInteger integer;
    private final Logger logger = LoggerFactory.getLogger(IntegerCounter.class);
    private TEndPoint leaderEndpoint;
    private int leaderId;
    private List<Peer> configuration;

    @Override
    public void start() {
      integer = new AtomicInteger(0);
    }

    @Override
    public void stop() {}

    @Override
    public TSStatus write(IConsensusRequest request) {
      if (((TestRequest) request).isIncr()) {
        integer.incrementAndGet();
      }
      return new TSStatus(200);
    }

    @Override
    public IConsensusRequest deserializeRequest(IConsensusRequest request) {
      TestRequest testRequest;
      if (request instanceof ByteBufferConsensusRequest) {
        testRequest = new TestRequest(request.serializeToByteBuffer());
      } else {
        testRequest = (TestRequest) request;
      }
      return testRequest;
    }

    @Override
    public DataSet read(IConsensusRequest IConsensusRequest) {
      TestDataSet dataSet = new TestDataSet();
      dataSet.setNumber(integer.get());
      return dataSet;
    }

    @Override
    public boolean takeSnapshot(File snapshotDir) {
      File snapshot = new File(snapshotDir.getAbsolutePath() + File.separator + "snapshot");
      try (FileWriter writer = new FileWriter(snapshot)) {
        writer.write(String.valueOf(integer.get()));
      } catch (IOException e) {
        logger.error("cannot open file writer of {}", snapshot);
        return false;
      }
      return true;
    }

    @Override
    public void loadSnapshot(File latestSnapshotRootDir) {
      File snapshot =
          new File(latestSnapshotRootDir.getAbsolutePath() + File.separator + "snapshot");
      try (Scanner scanner = new Scanner(snapshot)) {
        integer.set(Integer.parseInt(scanner.next()));
      } catch (FileNotFoundException e) {
        logger.error("cannot find snapshot file {}", snapshot);
      }
    }

    @Override
    public void notifyLeaderChanged(ConsensusGroupId groupId, int newLeaderId) {
      this.leaderId = newLeaderId;
      System.out.println("---------newLeader-----------");
      System.out.println(groupId);
      System.out.println(newLeaderId);
      System.out.println("----------------------");
    }

    @Override
    public void notifyConfigurationChanged(long term, long index, List<Peer> newConfiguration) {
      this.configuration = newConfiguration;
      System.out.println("----------newConfiguration------------");
      System.out.println("term : " + term);
      System.out.println("index : " + index);
      for (Peer peer : newConfiguration) {
        System.out.println(peer);
      }
      System.out.println("----------------------");
    }

    @TestOnly
    public static synchronized String ensureSnapshotFileName(File snapshotDir, String metadata) {
      File dir = new File(snapshotDir + File.separator + metadata);
      if (!(dir.exists() && dir.isDirectory())) {
        dir.mkdirs();
      }
      return dir.getPath() + File.separator + "snapshot";
    }

    public TEndPoint getLeaderEndpoint() {
      return leaderEndpoint;
    }

    public List<Peer> getConfiguration() {
      return configuration;
    }
  }
}
