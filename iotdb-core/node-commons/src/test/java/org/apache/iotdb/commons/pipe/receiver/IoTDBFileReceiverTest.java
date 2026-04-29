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

package org.apache.iotdb.commons.pipe.receiver;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeRequestType;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferFileSealReqV1;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferFileSealReqV2;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class IoTDBFileReceiverTest {

  @Test
  public void testRejectPathTraversalFileName() throws Exception {
    final Path baseDir = Files.createTempDirectory("iotdb-file-receiver-test");
    final DummyFileReceiver receiver = new DummyFileReceiver(baseDir.toFile());
    try {
      final IOException exception =
          Assert.assertThrows(
              IOException.class, () -> receiver.createWritingFile("../outside.tsfile", true));
      Assert.assertTrue(exception.getMessage().contains("Illegal fileName"));
    } finally {
      receiver.handleExit();
    }
  }

  @Test
  public void testAllowNormalFileName() throws Exception {
    final Path baseDir = Files.createTempDirectory("iotdb-file-receiver-test");
    final DummyFileReceiver receiver = new DummyFileReceiver(baseDir.toFile());
    try {
      receiver.createWritingFile("normal.tsfile", true);
      Assert.assertTrue(receiver.getWritingFileInBaseDir("normal.tsfile").exists());
    } finally {
      receiver.handleExit();
    }
  }

  @Test
  public void testRejectPathTraversalFileNameInSealRequest() throws Exception {
    final Path baseDir = Files.createTempDirectory("iotdb-file-receiver-test");
    final DummyFileReceiver receiver = new DummyFileReceiver(baseDir.toFile());
    try {
      receiver.createWritingFile("normal.tsfile", false);

      final TPipeTransferResp response =
          receiver.sealFiles(
              Arrays.asList("../outside.mod", "normal.tsfile"), Arrays.asList(0L, 0L));

      Assert.assertEquals(
          TSStatusCode.PIPE_TRANSFER_FILE_ERROR.getStatusCode(), response.getStatus().getCode());
      Assert.assertTrue(response.getStatus().getMessage().contains("Illegal fileName"));
    } finally {
      receiver.handleExit();
    }
  }

  private static class DummyFileReceiver extends IoTDBFileReceiver {

    DummyFileReceiver(final File baseDir) {
      receiverFileDirWithIdSuffix.set(baseDir);
    }

    void createWritingFile(final String fileName, final boolean isSingleFile) throws IOException {
      updateWritingFileIfNeeded(fileName, isSingleFile);
    }

    TPipeTransferResp sealFiles(final List<String> fileNames, final List<Long> fileLengths)
        throws IOException {
      return handleTransferFileSealV2(
          DummyFileSealReqV2.toTPipeTransferReq(fileNames, fileLengths, Collections.emptyMap()));
    }

    File getWritingFileInBaseDir(final String fileName) {
      return receiverFileDirWithIdSuffix.get().toPath().resolve(fileName).toFile();
    }

    @Override
    protected String getReceiverFileBaseDir() {
      return receiverFileDirWithIdSuffix.get().getAbsolutePath();
    }

    @Override
    protected String getSenderHost() {
      return "127.0.0.1";
    }

    @Override
    protected String getSenderPort() {
      return "6667";
    }

    @Override
    protected String getClusterId() {
      return "test-cluster";
    }

    @Override
    protected TSStatus login() {
      return new TSStatus(200);
    }

    @Override
    protected TSStatus loadFileV1(
        final PipeTransferFileSealReqV1 req, final String fileAbsolutePath) {
      return new TSStatus(200);
    }

    @Override
    protected TSStatus loadFileV2(
        final PipeTransferFileSealReqV2 req, final List<String> fileAbsolutePaths)
        throws IllegalPathException {
      return new TSStatus(200);
    }

    @Override
    protected void closeSession() {
      // noop for unit test
    }

    @Override
    public TPipeTransferResp receive(TPipeTransferReq req) {
      return null;
    }
  }

  private static class DummyFileSealReqV2 extends PipeTransferFileSealReqV2 {

    static DummyFileSealReqV2 toTPipeTransferReq(
        final List<String> fileNames,
        final List<Long> fileLengths,
        final java.util.Map<String, String> parameters)
        throws IOException {
      return (DummyFileSealReqV2)
          new DummyFileSealReqV2().convertToTPipeTransferReq(fileNames, fileLengths, parameters);
    }

    @Override
    protected PipeRequestType getPlanType() {
      return PipeRequestType.TRANSFER_SCHEMA_SNAPSHOT_SEAL;
    }
  }
}
