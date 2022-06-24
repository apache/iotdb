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

import org.apache.ratis.io.MD5Hash;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.util.MD5FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;

/**
 * When Leader sends snapshots to a follower, it will send the file itself together with md5 digest.
 * When snapshot file is large, this computation incurs significant overhead. This class implements
 * a strategy that delay the computing when the md5 info is first fetched.
 *
 * <p>TODO A better strategy is to calculate the md5 along when sending the InstallSnapshot request
 * with file chunks.
 */
public class FileInfoWithDelayedMd5Computing extends FileInfo {

  private static final Logger logger =
      LoggerFactory.getLogger(FileInfoWithDelayedMd5Computing.class);
  private volatile MD5Hash digest;

  public FileInfoWithDelayedMd5Computing(Path path, MD5Hash fileDigest) {
    super(path, fileDigest);
    digest = null;
  }

  public FileInfoWithDelayedMd5Computing(Path path) {
    this(path, null);
  }

  // return null iff sync md5 computing failed
  @Override
  public MD5Hash getFileDigest() {
    if (digest == null) {
      synchronized (this) {
        if (digest == null) {
          try {
            if (MD5FileUtil.getDigestFileForFile(getPath().toFile()).exists()) {
              digest = MD5FileUtil.readStoredMd5ForFile(getPath().toFile());
            }
            digest = MD5FileUtil.computeMd5ForFile(getPath().toFile());
            MD5FileUtil.saveMD5File(getPath().toFile(), digest);
          } catch (IOException ioException) {
            logger.error("compute file digest for {} failed due to {}", getPath(), ioException);
            return null;
          }
        }
      }
    }
    return digest;
  }
}
