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

package org.apache.iotdb.cluster.log.snapshot;

import org.apache.iotdb.cluster.client.async.AsyncDataClient;
import org.apache.iotdb.cluster.client.sync.SyncDataClient;
import org.apache.iotdb.cluster.common.TestDataGroupMember;
import org.apache.iotdb.cluster.common.TestLogManager;
import org.apache.iotdb.cluster.common.TestMetaGroupMember;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.coordinator.Coordinator;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.Client;
import org.apache.iotdb.cluster.server.member.DataGroupMember;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.cluster.utils.IOUtils;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.apache.thrift.TConfiguration;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;

public abstract class DataSnapshotTest {

  DataGroupMember dataGroupMember;
  MetaGroupMember metaGroupMember;
  Coordinator coordinator;
  final int failureFrequency = 10;
  int failureCnt;
  boolean addNetFailure = false;

  private final ClusterConfig config = ClusterDescriptor.getInstance().getConfig();
  private boolean isAsyncServer;

  @Before
  public void setUp() throws MetadataException, StartupException {
    isAsyncServer = config.isUseAsyncServer();
    config.setUseAsyncServer(true);
    dataGroupMember =
        new TestDataGroupMember() {
          @Override
          public AsyncClient getAsyncClient(Node node, boolean activatedOnly) {
            return getAsyncClient(node);
          }

          @Override
          public AsyncClient getAsyncClient(Node node) {
            return new AsyncDataClient(null, null, null) {
              @Override
              public void readFile(
                  String filePath,
                  long offset,
                  int length,
                  AsyncMethodCallback<ByteBuffer> resultHandler) {
                new Thread(
                        () -> {
                          if (addNetFailure && (failureCnt++) % failureFrequency == 0) {
                            // insert 1 failure in every 10 requests
                            resultHandler.onError(new Exception("Faked network failure"));
                            return;
                          }
                          try {
                            resultHandler.onComplete(IOUtils.readFile(filePath, offset, length));
                          } catch (IOException e) {
                            resultHandler.onError(e);
                          }
                        })
                    .start();
              }

              @Override
              public void removeHardLink(
                  String hardLinkPath, AsyncMethodCallback<Void> resultHandler) {
                new Thread(
                        () -> {
                          try {
                            Files.deleteIfExists(new File(hardLinkPath).toPath());
                          } catch (IOException e) {
                            // ignore
                          }
                        })
                    .start();
              }
            };
          }

          @Override
          public Client getSyncClient(Node node) {
            return new SyncDataClient(
                new TBinaryProtocol(
                    new TTransport() {
                      @Override
                      public boolean isOpen() {
                        return false;
                      }

                      @Override
                      public void open() {}

                      @Override
                      public void close() {}

                      @Override
                      public int read(byte[] bytes, int i, int i1) {
                        return 0;
                      }

                      @Override
                      public void write(byte[] bytes, int i, int i1) {}

                      @Override
                      public TConfiguration getConfiguration() {
                        return null;
                      }

                      @Override
                      public void updateKnownMessageSize(long size) {}

                      @Override
                      public void checkReadBytesAvailable(long numBytes) {}
                    })) {
              @Override
              public ByteBuffer readFile(String filePath, long offset, int length)
                  throws TException {
                if (addNetFailure && (failureCnt++) % failureFrequency == 0) {
                  // simulate failures
                  throw new TException("Faked network failure");
                }
                try {
                  return IOUtils.readFile(filePath, offset, length);
                } catch (IOException e) {
                  throw new TException(e);
                }
              }
            };
          }
        };
    // do nothing
    metaGroupMember =
        new TestMetaGroupMember() {
          @Override
          public void syncLeaderWithConsistencyCheck(boolean isWriteRequest) {
            // do nothing
          }
        };
    coordinator = new Coordinator(metaGroupMember);
    metaGroupMember.setCoordinator(coordinator);
    metaGroupMember.setPartitionTable(TestUtils.getPartitionTable(10));
    dataGroupMember.setMetaGroupMember(metaGroupMember);
    dataGroupMember.setLogManager(new TestLogManager(0));
    EnvironmentUtils.envSetUp();
    for (int i = 0; i < 10; i++) {
      IoTDB.metaManager.setStorageGroup(new PartialPath(TestUtils.getTestSg(i)));
    }
    addNetFailure = false;
  }

  @After
  public void tearDown() throws Exception {
    config.setUseAsyncServer(isAsyncServer);
    metaGroupMember.closeLogManager();
    dataGroupMember.closeLogManager();
    metaGroupMember.stop();
    dataGroupMember.stop();
    EnvironmentUtils.cleanEnv();
  }
}
