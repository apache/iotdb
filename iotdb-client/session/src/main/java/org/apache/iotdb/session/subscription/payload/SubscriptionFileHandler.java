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

package org.apache.iotdb.session.subscription.payload;

import org.apache.iotdb.session.util.RetryUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

public abstract class SubscriptionFileHandler implements SubscriptionMessageHandler {

  protected final String absolutePath;

  public SubscriptionFileHandler(final String absolutePath) {
    this.absolutePath = absolutePath;
  }

  /**
   * @return a new File instance of the corresponding file
   */
  public synchronized File getFile() {
    return new File(absolutePath);
  }

  /**
   * @return a new Path instance of the corresponding file
   */
  public synchronized Path getPath() {
    return Paths.get(absolutePath);
  }

  /**
   * @return the path to the source file
   * @throws IOException if an I/O error occurs
   */
  public synchronized Path deleteFile() throws IOException {
    final Path sourcePath = getPath();
    return RetryUtils.retryOnException(
        () -> {
          Files.delete(sourcePath);
          return sourcePath;
        });
  }

  /**
   * @param target the path to the target file
   * @return the path to the target file
   * @throws IOException if an I/O error occurs
   */
  public synchronized Path moveFile(final String target) throws IOException {
    return RetryUtils.retryOnException(() -> this.moveFile(Paths.get(target)));
  }

  /**
   * @param target the path to the target file
   * @return the path to the target file
   * @throws IOException if an I/O error occurs
   */
  public synchronized Path moveFile(final Path target) throws IOException {
    if (!Files.exists(target.getParent())) {
      Files.createDirectories(target.getParent());
    }
    return RetryUtils.retryOnException(
        () -> Files.move(getPath(), target, StandardCopyOption.REPLACE_EXISTING));
  }

  /**
   * @param target the path to the target file
   * @return the path to the target file
   * @throws IOException if an I/O error occurs
   */
  public synchronized Path copyFile(final String target) throws IOException {
    return RetryUtils.retryOnException(() -> this.copyFile(Paths.get(target)));
  }

  /**
   * @param target the path to the target file
   * @return the path to the target file
   * @throws IOException if an I/O error occurs
   */
  public synchronized Path copyFile(final Path target) throws IOException {
    if (!Files.exists(target.getParent())) {
      Files.createDirectories(target.getParent());
    }
    return RetryUtils.retryOnException(
        () ->
            Files.copy(
                getPath(),
                target,
                StandardCopyOption.REPLACE_EXISTING,
                StandardCopyOption.COPY_ATTRIBUTES));
  }
}
