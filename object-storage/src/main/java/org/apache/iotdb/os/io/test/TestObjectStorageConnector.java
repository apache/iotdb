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
package org.apache.iotdb.os.io.test;

import org.apache.iotdb.os.conf.ObjectStorageDescriptor;
import org.apache.iotdb.os.conf.provider.TestConfig;
import org.apache.iotdb.os.exception.ObjectStorageException;
import org.apache.iotdb.os.fileSystem.OSURI;
import org.apache.iotdb.os.io.IMetaData;
import org.apache.iotdb.os.io.ObjectStorageConnector;
import org.apache.iotdb.os.io.aws.S3MetaData;
import org.apache.iotdb.os.utils.ObjectStorageConstant;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

public class TestObjectStorageConnector implements ObjectStorageConnector {
  private final TestConfig testConfig =
      (TestConfig) ObjectStorageDescriptor.getInstance().getConfig().getProviderConfig();

  @Override
  public boolean doesObjectExist(OSURI osUri) throws ObjectStorageException {
    File file = new File(getDstFilePath(osUri));
    return file.exists();
  }

  @Override
  public IMetaData getMetaData(OSURI osUri) throws ObjectStorageException {
    File file = new File(getDstFilePath(osUri));
    return new S3MetaData(file.length(), System.currentTimeMillis());
  }

  @Override
  public boolean createNewEmptyObject(OSURI osUri) throws ObjectStorageException {
    File file = new File(getDstFilePath(osUri));
    if (!file.exists()) {
      try {
        return file.createNewFile();
      } catch (IOException e) {
        throw new ObjectStorageException(e);
      }
    }
    return false;
  }

  @Override
  public boolean delete(OSURI osUri) throws ObjectStorageException {
    File file = new File(getDstFilePath(osUri));
    return file.delete();
  }

  @Override
  public boolean renameTo(OSURI fromOSUri, OSURI toOSUri) throws ObjectStorageException {
    File file = new File(getDstFilePath(fromOSUri));
    return file.renameTo(new File(getDstFilePath(toOSUri)));
  }

  @Override
  public InputStream getInputStream(OSURI osUri) throws ObjectStorageException {
    File file = new File(getDstFilePath(osUri));
    try {
      return Channels.newInputStream(FileChannel.open(file.toPath(), StandardOpenOption.READ));
    } catch (IOException e) {
      throw new ObjectStorageException(e);
    }
  }

  @Override
  public OSURI[] list(OSURI osUri) throws ObjectStorageException {
    return null;
  }

  @Override
  public void putLocalFile(OSURI osUri, File localFile) throws ObjectStorageException {
    try {
      File targetFile = new File(getDstFilePath(osUri));
      if (!targetFile.getParentFile().exists() && !targetFile.getParentFile().mkdirs()) {
        throw new ObjectStorageException(
            String.format(
                "[TieredMigration] cannot mkdir for path %s",
                targetFile.getParentFile().getAbsolutePath()));
      }
      FileUtils.copyFile(localFile, targetFile);
    } catch (IOException e) {
      throw new ObjectStorageException(e);
    }
  }

  @Override
  public byte[] getRemoteObject(OSURI osUri, long position, int len) throws ObjectStorageException {
    File file = new File(getDstFilePath(osUri));
    ByteBuffer dst = ByteBuffer.allocate(len);
    try (FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
      channel.read(dst, position);
    } catch (IOException e) {
      throw new ObjectStorageException(e);
    }
    return dst.array();
  }

  @Override
  public void deleteObjectsByPrefix(OSURI prefixUri) throws ObjectStorageException {
    File file = new File(getDstFilePath(prefixUri));
    try {
      FileUtils.deleteDirectory(file);
    } catch (IOException e) {
      throw new ObjectStorageException(e);
    }
  }

  private String getDstFilePath(OSURI osuri) {
    return testConfig.getTestDir()
        + File.separator
        + osuri.getKey().replace(ObjectStorageConstant.FILE_SEPARATOR, File.separator);
  }

  @Override
  public void copyObject(OSURI srcUri, OSURI destUri) throws ObjectStorageException {}
}
