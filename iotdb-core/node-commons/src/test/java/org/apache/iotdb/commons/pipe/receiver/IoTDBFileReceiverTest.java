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
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeOutOfMemoryCriticalException;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.common.PipeTransferHandshakeConstant;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeRequestType;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferFilePieceReq;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferFileSealReqV1;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferFileSealReqV2;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferHandshakeV1Req;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferHandshakeV2Req;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.response.PipeTransferFilePieceResp;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

  @Test
  public void testHandshakeResetsWritingFileState() throws Exception {
    final Path baseDir = Files.createTempDirectory("iotdb-file-receiver-test");
    final DummyFileReceiver receiver = new DummyFileReceiver(baseDir.toFile());
    try {
      receiver.handshake();
      receiver.createWritingFile("normal.tsfile", true);
      receiver.writeToCurrentWritingFile(new byte[] {1, 2, 3});

      final File oldReceiverDir = receiver.getCurrentReceiverDir();
      Assert.assertNotNull(receiver.getCurrentWritingFile());
      Assert.assertNotNull(receiver.getCurrentWritingFileWriter());

      receiver.handshake();

      Assert.assertFalse(oldReceiverDir.exists());
      Assert.assertNull(receiver.getCurrentWritingFile());
      Assert.assertNull(receiver.getCurrentWritingFileWriter());
      Assert.assertNotEquals(
          oldReceiverDir.getAbsolutePath(), receiver.getCurrentReceiverDir().getAbsolutePath());
    } finally {
      receiver.handleExit();
    }
  }

  @Test
  public void testHandshakeV1ClearsPipeCredential() throws Exception {
    final Path baseDir = Files.createTempDirectory("iotdb-file-receiver-test");
    final DummyFileReceiver receiver = new DummyFileReceiver(baseDir.toFile());
    try {
      receiver.setHasPipeHandshakeCredential(true);

      receiver.handshake();

      Assert.assertFalse(receiver.hasPipeHandshakeCredential());
    } finally {
      receiver.handleExit();
    }
  }

  @Test
  public void testHandshakeV2RequiresCredentials() throws Exception {
    final Path baseDir = Files.createTempDirectory("iotdb-file-receiver-test");
    final DummyFileReceiver receiver = new DummyFileReceiver(baseDir.toFile());
    try {
      final TPipeTransferResp response = receiver.handshakeV2(buildHandshakeV2Params(false));

      Assert.assertEquals(TSStatusCode.NOT_LOGIN.getStatusCode(), response.getStatus().getCode());
      Assert.assertEquals(0, receiver.getLoginCallCount());
    } finally {
      receiver.handleExit();
    }
  }

  @Test
  public void testHandshakeV2AuthenticatesImmediately() throws Exception {
    final Path baseDir = Files.createTempDirectory("iotdb-file-receiver-test");
    final DummyFileReceiver receiver = new DummyFileReceiver(baseDir.toFile());
    try {
      final TPipeTransferResp response = receiver.handshakeV2(buildHandshakeV2Params(true));

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), response.getStatus().getCode());
      Assert.assertEquals(1, receiver.getLoginCallCount());
      Assert.assertTrue(receiver.hasPipeHandshakeCredential());
    } finally {
      receiver.handleExit();
    }
  }

  private Map<String, String> buildHandshakeV2Params(final boolean includeCredentials) {
    final Map<String, String> params = new HashMap<>();
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_CLUSTER_ID, "sender-cluster");
    params.put(
        PipeTransferHandshakeConstant.HANDSHAKE_KEY_TIME_PRECISION,
        CommonDescriptor.getInstance().getConfig().getTimestampPrecision());
    if (includeCredentials) {
      params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_USERNAME, "root");
      params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_PASSWORD, "root");
    }
    return params;
  }

  @Test
  public void testSealFileV1FailureDeletesTransferredFile() throws Exception {
    final Path baseDir = Files.createTempDirectory("iotdb-file-receiver-test");
    final DummyFileReceiver receiver = new DummyFileReceiver(baseDir.toFile());
    try {
      receiver.createWritingFile("normal.tsfile", true);
      receiver.writeToCurrentWritingFile(new byte[] {1, 2, 3});
      receiver.setLoadFileV1Status(
          new TSStatus(TSStatusCode.PIPE_TRANSFER_FILE_ERROR.getStatusCode()));

      final File transferredFile = receiver.getWritingFileInBaseDir("normal.tsfile");
      final TPipeTransferResp response = receiver.sealFileV1("normal.tsfile", 3L);

      Assert.assertEquals(
          TSStatusCode.PIPE_TRANSFER_FILE_ERROR.getStatusCode(), response.getStatus().getCode());
      Assert.assertFalse(transferredFile.exists());
      Assert.assertNull(receiver.getCurrentWritingFile());
      Assert.assertNull(receiver.getCurrentWritingFileWriter());
    } finally {
      receiver.handleExit();
    }
  }

  @Test
  public void testFilePieceMemoryAllocationFailureReturnsTemporaryUnavailable() throws Exception {
    final Path baseDir = Files.createTempDirectory("iotdb-file-receiver-test");
    final DummyFileReceiver receiver = new DummyFileReceiver(baseDir.toFile());
    try {
      receiver.setFailFilePieceMemoryAllocation(true);

      final TPipeTransferResp response =
          receiver.writeFilePiece("normal.tsfile", 0, new byte[] {1, 2, 3});
      final PipeTransferFilePieceResp filePieceResp =
          PipeTransferFilePieceResp.fromTPipeTransferResp(response);

      Assert.assertEquals(
          TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode(),
          response.getStatus().getCode());
      Assert.assertTrue(response.getStatus().getMessage().contains("no memory for file piece"));
      Assert.assertEquals(
          PipeTransferFilePieceResp.ERROR_END_OFFSET, filePieceResp.getEndWritingOffset());
      Assert.assertFalse(receiver.getWritingFileInBaseDir("normal.tsfile").exists());
    } finally {
      receiver.handleExit();
    }
  }

  @Test
  public void testFilePieceMemoryAllocationIsClosedAfterWrite() throws Exception {
    final Path baseDir = Files.createTempDirectory("iotdb-file-receiver-test");
    final DummyFileReceiver receiver = new DummyFileReceiver(baseDir.toFile());
    try {
      final TPipeTransferResp response =
          receiver.writeFilePiece("normal.tsfile", 0, new byte[] {1, 2, 3});
      final PipeTransferFilePieceResp filePieceResp =
          PipeTransferFilePieceResp.fromTPipeTransferResp(response);

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), response.getStatus().getCode());
      Assert.assertEquals(3, filePieceResp.getEndWritingOffset());
      Assert.assertEquals(1, receiver.getFilePieceMemoryCloseCount());
    } finally {
      receiver.handleExit();
    }
  }

  private static class DummyFileReceiver extends IoTDBFileReceiver {

    private final File receiverFileBaseDir;
    private TSStatus loadFileV1Status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    private int loginCallCount = 0;
    private boolean failFilePieceMemoryAllocation = false;
    private int filePieceMemoryCloseCount = 0;

    DummyFileReceiver(final File baseDir) {
      receiverFileBaseDir = baseDir;
      receiverFileDirWithIdSuffix.set(baseDir);
    }

    void createWritingFile(final String fileName, final boolean isSingleFile) throws IOException {
      updateWritingFileIfNeeded(fileName, isSingleFile);
    }

    void handshake() throws IOException {
      handleTransferHandshakeV1(
          DummyHandshakeReq.toTPipeTransferReq(
              CommonDescriptor.getInstance().getConfig().getTimestampPrecision()));
    }

    TPipeTransferResp handshakeV2(final Map<String, String> params) throws IOException {
      return handleTransferHandshakeV2(DummyHandshakeV2Req.toTPipeTransferReq(params));
    }

    void writeToCurrentWritingFile(final byte[] bytes) throws Exception {
      getCurrentWritingFileWriter().write(bytes);
    }

    void setLoadFileV1Status(final TSStatus status) {
      loadFileV1Status = status;
    }

    void setHasPipeHandshakeCredential(final boolean hasPipeHandshakeCredential) {
      this.hasPipeHandshakeCredential = hasPipeHandshakeCredential;
    }

    boolean hasPipeHandshakeCredential() {
      return hasPipeHandshakeCredential;
    }

    int getLoginCallCount() {
      return loginCallCount;
    }

    void setFailFilePieceMemoryAllocation(final boolean failFilePieceMemoryAllocation) {
      this.failFilePieceMemoryAllocation = failFilePieceMemoryAllocation;
    }

    int getFilePieceMemoryCloseCount() {
      return filePieceMemoryCloseCount;
    }

    TPipeTransferResp writeFilePiece(
        final String fileName, final long startWritingOffset, final byte[] filePiece)
        throws IOException {
      return handleTransferFilePiece(
          DummyFilePieceReq.toTPipeTransferReq(fileName, startWritingOffset, filePiece),
          false,
          true);
    }

    TPipeTransferResp sealFileV1(final String fileName, final long fileLength) throws IOException {
      return handleTransferFileSealV1(DummyFileSealReqV1.toTPipeTransferReq(fileName, fileLength));
    }

    TPipeTransferResp sealFiles(final List<String> fileNames, final List<Long> fileLengths)
        throws IOException {
      return handleTransferFileSealV2(
          DummyFileSealReqV2.toTPipeTransferReq(fileNames, fileLengths, Collections.emptyMap()));
    }

    File getWritingFileInBaseDir(final String fileName) {
      return receiverFileDirWithIdSuffix.get().toPath().resolve(fileName).toFile();
    }

    File getCurrentReceiverDir() {
      return receiverFileDirWithIdSuffix.get();
    }

    File getCurrentWritingFile() throws Exception {
      return (File) getField("writingFile").get(this);
    }

    RandomAccessFile getCurrentWritingFileWriter() throws Exception {
      return (RandomAccessFile) getField("writingFileWriter").get(this);
    }

    private Field getField(final String fieldName) throws NoSuchFieldException {
      final Field field = IoTDBFileReceiver.class.getDeclaredField(fieldName);
      field.setAccessible(true);
      return field;
    }

    @Override
    protected String getReceiverFileBaseDir() {
      return receiverFileBaseDir.getAbsolutePath();
    }

    @Override
    protected void markFileBaseDirStateAbnormal(final String dir) {
      // noop for unit test
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
      loginCallCount++;
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }

    @Override
    protected AutoCloseable tryAllocateMemoryForFilePiece(final PipeTransferFilePieceReq req) {
      if (failFilePieceMemoryAllocation) {
        throw new PipeRuntimeOutOfMemoryCriticalException("no memory for file piece");
      }
      return () -> filePieceMemoryCloseCount++;
    }

    @Override
    protected TSStatus loadFileV1(
        final PipeTransferFileSealReqV1 req, final String fileAbsolutePath) {
      return loadFileV1Status;
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

  private static class DummyFilePieceReq extends PipeTransferFilePieceReq {

    static DummyFilePieceReq toTPipeTransferReq(
        final String fileName, final long startWritingOffset, final byte[] filePiece)
        throws IOException {
      return (DummyFilePieceReq)
          new DummyFilePieceReq()
              .convertToTPipeTransferReq(fileName, startWritingOffset, filePiece);
    }

    @Override
    protected PipeRequestType getPlanType() {
      return PipeRequestType.TRANSFER_TS_FILE_PIECE;
    }
  }

  private static class DummyHandshakeReq extends PipeTransferHandshakeV1Req {

    static DummyHandshakeReq toTPipeTransferReq(final String timestampPrecision)
        throws IOException {
      return (DummyHandshakeReq)
          new DummyHandshakeReq().convertToTPipeTransferReq(timestampPrecision);
    }

    @Override
    protected PipeRequestType getPlanType() {
      return PipeRequestType.HANDSHAKE_DATANODE_V1;
    }
  }

  private static class DummyHandshakeV2Req extends PipeTransferHandshakeV2Req {

    static DummyHandshakeV2Req toTPipeTransferReq(final Map<String, String> params)
        throws IOException {
      return (DummyHandshakeV2Req) new DummyHandshakeV2Req().convertToTPipeTransferReq(params);
    }

    @Override
    protected PipeRequestType getPlanType() {
      return PipeRequestType.HANDSHAKE_DATANODE_V2;
    }
  }

  private static class DummyFileSealReqV1 extends PipeTransferFileSealReqV1 {

    static DummyFileSealReqV1 toTPipeTransferReq(final String fileName, final long fileLength)
        throws IOException {
      return (DummyFileSealReqV1)
          new DummyFileSealReqV1().convertToTPipeTransferReq(fileName, fileLength);
    }

    @Override
    protected PipeRequestType getPlanType() {
      return PipeRequestType.TRANSFER_TS_FILE_SEAL;
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
