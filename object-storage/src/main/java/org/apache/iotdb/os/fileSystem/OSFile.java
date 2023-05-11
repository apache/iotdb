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
package org.apache.iotdb.os.fileSystem;

import java.io.FileFilter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import org.apache.iotdb.os.conf.ObjectStorageConfig;
import org.apache.iotdb.os.conf.ObjectStorageDescriptor;
import org.apache.iotdb.os.exception.ObjectStorageException;
import org.apache.iotdb.os.io.ObjectStorageConnector;
import org.apache.iotdb.os.io.aws.S3ObjectStorageConnector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;

import static org.apache.iotdb.os.utils.ObjectStorageConstant.FILE_SEPARATOR;


public class OSFile extends File {
  private static final Logger logger = LoggerFactory.getLogger(OSFile.class);
  private static final String UNSUPPORT_OPERATION = "Current object storage file doesn't support this operation.";
  private static final ObjectStorageConfig config =
      ObjectStorageDescriptor.getInstance().getConfig();
  private static final ObjectStorageConnector connector;

  static {
    switch (config.getOsType()) {
      case AWS_S3:
        connector = new S3ObjectStorageConnector();
        break;
      default:
        connector = null;
    }
  }

  private final OSURI osUri;

  public OSFile(String pathname) {
    super(pathname);
    this.osUri = new OSURI(pathname);
  }

  public OSFile(String parent, String child) {
    super(parent, child);
    this.osUri = new OSURI(parent + FILE_SEPARATOR + child);
  }

  public OSFile(File parent, String child) {
    super(parent, child);
    this.osUri = new OSURI(parent.toURI() + FILE_SEPARATOR + child);
  }

  public OSFile(URI uri) {
    super(uri);
    this.osUri = new OSURI(uri);
  }

  public OSFile(OSURI osUri) {
    super(osUri.getURI());
    this.osUri = osUri;
  }

  @Override
  public String getName() {
    return osUri.getKey();
  }

  @Override
  public String getParent() {
    File parent = getParentFile();
    return parent == null ? null : parent.toString();
  }

  @Override
  public File getParentFile() {
    int lastSeparatorIdx = osUri.getKey().lastIndexOf(FILE_SEPARATOR);
    if(lastSeparatorIdx <= 0) {
      return null;
    }
    return new OSFile(new OSURI(osUri.getBucket(), osUri.getKey().substring(0, lastSeparatorIdx)));
  }

  @Override
  public String getPath() {
    return osUri.toString();
  }

  @Override
  public boolean isAbsolute() {
    return true;
  }

  @Override
  public String getAbsolutePath() {
    return osUri.toString();
  }

  @Override
  public File getAbsoluteFile() {
    return this;
  }

  @Override
  public String getCanonicalPath() throws IOException {
    return osUri.toString();
  }

  @Override
  public File getCanonicalFile() throws IOException {
    return this;
  }

  @Override
  public URL toURL() throws MalformedURLException {
    throw new UnsupportedOperationException(UNSUPPORT_OPERATION);
  }

  @Override
  public URI toURI() {
    return osUri.getURI();
  }

  @Override
  public boolean canRead() {
    return this.exists();
  }

  @Override
  public boolean canWrite() {
    return this.exists();
  }

  @Override
  public boolean exists() {
    try {
      return connector.doesObjectExist(osUri);
    } catch (ObjectStorageException e) {
      logger.error("Fail to get object {}.", osUri, e);
    }
    return false;
  }

  @Override
  public boolean isDirectory() {
    return false;
  }

  @Override
  public boolean isFile() {
    return true;
  }

  @Override
  public boolean isHidden() {
    throw new UnsupportedOperationException(UNSUPPORT_OPERATION);
  }

  @Override
  public long lastModified() {
    return super.lastModified();
  }

  @Override
  public long length() {
    return super.length();
  }

  @Override
  public boolean createNewFile() throws IOException {
    return super.createNewFile();
  }

  @Override
  public boolean delete() {
    return super.delete();
  }

  @Override
  public void deleteOnExit() {
    throw new UnsupportedOperationException(UNSUPPORT_OPERATION);
  }

  @Override
  public String[] list() {
    throw new UnsupportedOperationException(UNSUPPORT_OPERATION);
  }

  @Override
  public String[] list(FilenameFilter filter) {
    throw new UnsupportedOperationException(UNSUPPORT_OPERATION);
  }

  @Override
  public File[] listFiles() {
    return super.listFiles();
  }

  @Override
  public File[] listFiles(FilenameFilter filter) {
    throw new UnsupportedOperationException(UNSUPPORT_OPERATION);
  }

  @Override
  public File[] listFiles(FileFilter filter) {
    throw new UnsupportedOperationException(UNSUPPORT_OPERATION);
  }

  @Override
  public boolean mkdir() {
    throw new UnsupportedOperationException(UNSUPPORT_OPERATION);
  }

  @Override
  public boolean mkdirs() {
    throw new UnsupportedOperationException(UNSUPPORT_OPERATION);
  }

  @Override
  public boolean renameTo(File dest) {
    return super.renameTo(dest);
  }

  @Override
  public boolean setLastModified(long time) {
    throw new UnsupportedOperationException(UNSUPPORT_OPERATION);
  }

  @Override
  public boolean setReadOnly() {
    throw new UnsupportedOperationException(UNSUPPORT_OPERATION);
  }

  @Override
  public boolean setWritable(boolean writable, boolean ownerOnly) {
    throw new UnsupportedOperationException(UNSUPPORT_OPERATION);
  }

  @Override
  public boolean setWritable(boolean writable) {
    throw new UnsupportedOperationException(UNSUPPORT_OPERATION);
  }

  @Override
  public boolean setReadable(boolean readable, boolean ownerOnly) {
    throw new UnsupportedOperationException(UNSUPPORT_OPERATION);
  }

  @Override
  public boolean setReadable(boolean readable) {
    throw new UnsupportedOperationException(UNSUPPORT_OPERATION);
  }

  @Override
  public boolean setExecutable(boolean executable, boolean ownerOnly) {
    throw new UnsupportedOperationException(UNSUPPORT_OPERATION);
  }

  @Override
  public boolean setExecutable(boolean executable) {
    throw new UnsupportedOperationException(UNSUPPORT_OPERATION);
  }

  @Override
  public boolean canExecute() {
    throw new UnsupportedOperationException(UNSUPPORT_OPERATION);
  }

  @Override
  public long getTotalSpace() {
    throw new UnsupportedOperationException(UNSUPPORT_OPERATION);
  }

  @Override
  public long getFreeSpace() {
    throw new UnsupportedOperationException(UNSUPPORT_OPERATION);
  }

  @Override
  public long getUsableSpace() {
    throw new UnsupportedOperationException(UNSUPPORT_OPERATION);
  }

  @Override
  public int compareTo(File pathname) {
    return this.toString().compareTo(pathname.toString());
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof OSFile)) {
      return false;
    }
    OSFile other = (OSFile) obj;
    return osUri.equals(other.osUri);
  }

  @Override
  public int hashCode() {
    return osUri.hashCode();
  }

  @Override
  public String toString() {
    return osUri.toString();
  }

  @Override
  public Path toPath() {
    throw new UnsupportedOperationException(UNSUPPORT_OPERATION);
  }
}
