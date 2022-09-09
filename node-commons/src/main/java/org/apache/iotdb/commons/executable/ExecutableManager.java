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

import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class ExecutableManager {

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
}
