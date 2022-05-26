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

package org.apache.iotdb.commons.udf.service;

import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class UDFExecutableManager implements IService, SnapshotProcessor {

  private final String temporaryLibRoot;
  private final String udfLibRoot;

  private final AtomicLong requestCounter;

  private UDFExecutableManager(String temporaryLibRoot, String udfLibRoot) {
    this.temporaryLibRoot = temporaryLibRoot;
    this.udfLibRoot = udfLibRoot;

    requestCounter = new AtomicLong(0);
  }

  public UDFExecutableResource request(List<String> uris) throws URISyntaxException, IOException {
    final long requestId = generateNextRequestId();
    downloadExecutables(uris, requestId);
    return new UDFExecutableResource(requestId, getDirStringByRequestId(requestId));
  }

  public void moveToExtLibDir(UDFExecutableResource resource, String functionName)
      throws IOException {
    FileUtils.moveDirectory(
        getDirByRequestId(resource.getRequestId()), getDirByFunctionName(functionName));
  }

  public void removeFromTemporaryLibRoot(UDFExecutableResource resource) {
    removeFromTemporaryLibRoot(resource.getRequestId());
  }

  public void removeFromExtLibDir(String functionName) {
    FileUtils.deleteQuietly(getDirByFunctionName(functionName));
  }

  private synchronized long generateNextRequestId() throws IOException {
    long requestId = requestCounter.getAndIncrement();
    while (FileUtils.isDirectory(getDirByRequestId(requestId))) {
      requestId = requestCounter.getAndIncrement();
    }
    FileUtils.forceMkdir(getDirByRequestId(requestId));
    return requestId;
  }

  private void downloadExecutables(List<String> uris, long requestId)
      throws IOException, URISyntaxException {
    // TODO: para download
    try {
      for (String uriString : uris) {
        final URL url = new URI(uriString).toURL();
        final String fileName = uriString.substring(uriString.lastIndexOf("/") + 1);
        final String destination =
            temporaryLibRoot + File.separator + requestId + File.separator + fileName;
        FileUtils.copyURLToFile(url, FSFactoryProducer.getFSFactory().getFile(destination));
      }
    } catch (Exception e) {
      removeFromTemporaryLibRoot(requestId);
      throw e;
    }
  }

  private void removeFromTemporaryLibRoot(long requestId) {
    FileUtils.deleteQuietly(getDirByRequestId(requestId));
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // dir string and dir file generation
  /////////////////////////////////////////////////////////////////////////////////////////////////

  public File getDirByRequestId(long requestId) {
    return FSFactoryProducer.getFSFactory().getFile(getDirStringByRequestId(requestId));
  }

  public String getDirStringByRequestId(long requestId) {
    return temporaryLibRoot + File.separator + requestId + File.separator;
  }

  public File getDirByFunctionName(String functionName) {
    return FSFactoryProducer.getFSFactory().getFile(getDirStringByFunctionName(functionName));
  }

  public String getDirStringByFunctionName(String functionName) {
    return udfLibRoot + File.separator + functionName + File.separator;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // IService
  /////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public void start() throws StartupException {
    try {
      makeDirIfNecessary(temporaryLibRoot);
      makeDirIfNecessary(udfLibRoot);
    } catch (Exception e) {
      throw new StartupException(e);
    }
  }

  @Override
  public void stop() {
    // nothing to do
  }

  @Override
  public ServiceType getID() {
    return ServiceType.UDF_EXECUTABLE_MANAGER_SERVICE;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // singleton instance holder
  /////////////////////////////////////////////////////////////////////////////////////////////////

  private static UDFExecutableManager INSTANCE = null;

  public static synchronized UDFExecutableManager setupAndGetInstance(
      String temporaryLibRoot, String udfLibRoot) {
    if (INSTANCE == null) {
      INSTANCE = new UDFExecutableManager(temporaryLibRoot, udfLibRoot);
    }
    return INSTANCE;
  }

  private static void makeDirIfNecessary(String dir) throws IOException {
    File file = SystemFileFactory.INSTANCE.getFile(dir);
    if (file.exists() && file.isDirectory()) {
      return;
    }
    FileUtils.forceMkdir(file);
  }

  public static UDFExecutableManager getInstance() {
    return INSTANCE;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // SnapshotProcessor
  /////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws IOException {
    return SnapshotUtils.takeSnapshotForDir(
            temporaryLibRoot,
            snapshotDir.getAbsolutePath() + File.separator + "ext" + File.separator + "temporary")
        && SnapshotUtils.takeSnapshotForDir(
            udfLibRoot,
            snapshotDir.getAbsolutePath() + File.separator + "ext" + File.separator + "udf");
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws IOException {
    SnapshotUtils.loadSnapshotForDir(
        snapshotDir.getAbsolutePath() + File.separator + "ext" + File.separator + "temporary",
        temporaryLibRoot);
    SnapshotUtils.loadSnapshotForDir(
        snapshotDir.getAbsolutePath() + File.separator + "ext" + File.separator + "udf",
        udfLibRoot);
  }
}
