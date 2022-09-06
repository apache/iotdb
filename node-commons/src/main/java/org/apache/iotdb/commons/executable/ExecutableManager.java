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

package org.apache.iotdb.commons.executable;

import org.apache.iotdb.commons.trigger.exception.TriggerJarToLargeException;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class ExecutableManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExecutableManager.class);

  protected final String temporaryLibRoot;
  protected final String libRoot;

  protected final AtomicLong requestCounter;

  public ExecutableManager(String temporaryLibRoot, String libRoot) {
    this.temporaryLibRoot = temporaryLibRoot;
    this.libRoot = libRoot;

    requestCounter = new AtomicLong(0);
  }

  public ExecutableResource request(List<String> uris) throws URISyntaxException, IOException {
    final long requestId = generateNextRequestId();
    downloadExecutables(uris, requestId);
    return new ExecutableResource(requestId, getDirStringByRequestId(requestId));
  }

  public void moveToExtLibDir(ExecutableResource resource, String name) throws IOException {
    FileUtils.moveDirectory(getDirByRequestId(resource.getRequestId()), getDirByName(name));
  }

  public void removeFromTemporaryLibRoot(ExecutableResource resource) {
    removeFromTemporaryLibRoot(resource.getRequestId());
  }

  public void removeFromExtLibDir(String functionName) {
    FileUtils.deleteQuietly(getDirByName(functionName));
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

  public File getDirByName(String name) {
    return FSFactoryProducer.getFSFactory().getFile(getDirStringByName(name));
  }

  public String getDirStringByName(String name) {
    return libRoot + File.separator + name + File.separator;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // transfer jar file to bytebuffer for thrift
  /////////////////////////////////////////////////////////////////////////////////////////////////

  public static ByteBuffer transferToBytebuffer(String filePath) throws IOException {
    try (FileChannel fileChannel = FileChannel.open(Paths.get(filePath), StandardOpenOption.READ)) {
      long size = fileChannel.size();
      if (size > Integer.MAX_VALUE) {
        // Max length of Thrift Binary is Integer.MAX_VALUE bytes.
        throw new TriggerJarToLargeException(
            String.format("Size of file exceed %d bytes", Integer.MAX_VALUE));
      }
      ByteBuffer byteBuffer = ByteBuffer.allocate((int) size);
      fileChannel.read(byteBuffer);
      return byteBuffer;
    } catch (Exception e) {
      LOGGER.warn(
          "Error occurred during transferring file{} to ByteBuffer, the cause is {}",
          filePath,
          e.getMessage());
      throw e;
    }
  }

  /**
   * @param byteBuffer jar data
   * @param fileName The name of the file. Absolute Path will be libRoot + File_Separator + fileName
   */
  public void writeToLibDir(ByteBuffer byteBuffer, String fileName) throws IOException {
    String destination = this.libRoot + File.separator + fileName;
    Path path = Paths.get(destination);
    Files.deleteIfExists(path);
    try (FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.WRITE)) {
      fileChannel.write(byteBuffer);
    } catch (IOException e) {
      LOGGER.warn(
          "Error occurred during writing bytebuffer to {} , the cause is {}",
          destination,
          e.getMessage());
      throw e;
    }
  }
}
