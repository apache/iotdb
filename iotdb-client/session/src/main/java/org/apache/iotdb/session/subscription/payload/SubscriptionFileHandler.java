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

import org.apache.iotdb.rpc.subscription.exception.SubscriptionIncompatibleHandlerException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

public abstract class SubscriptionFileHandler implements SubscriptionMessageHandler {

  protected final String filePath;

  public SubscriptionFileHandler(final String filePath) {
    this.filePath = filePath;
  }

  /**
   * @return a new File instance of the corresponding file
   */
  public File getFile() {
    return new File(filePath);
  }

  /**
   * @return a new Path instance of the corresponding file
   */
  public Path getPath() {
    return Paths.get(filePath);
  }

  /**
   * @throws IOException if an I/O error occurs
   */
  public void deleteFile() throws IOException {
    Files.delete(getPath());
  }

  /**
   * @param targetFilePath the path to the target file
   * @return the path to the target file
   * @throws IOException if an I/O error occurs
   */
  public Path moveFile(final String targetFilePath) throws IOException {
    return this.moveFile(Paths.get(targetFilePath));
  }

  /**
   * @param targetFilePath the path to the target file
   * @return the path to the target file
   * @throws IOException if an I/O error occurs
   */
  public Path moveFile(final Path targetFilePath) throws IOException {
    return Files.move(getPath(), targetFilePath, StandardCopyOption.REPLACE_EXISTING);
  }

  /**
   * @param targetFilePath the path to the target file
   * @return the path to the target file
   * @throws IOException if an I/O error occurs
   */
  public Path copyFile(final String targetFilePath) throws IOException {
    return this.copyFile(Paths.get(targetFilePath));
  }

  /**
   * @param targetFilePath the path to the target file
   * @return the path to the target file
   * @throws IOException if an I/O error occurs
   */
  public Path copyFile(final Path targetFilePath) throws IOException {
    return Files.copy(
        getPath(),
        targetFilePath,
        StandardCopyOption.REPLACE_EXISTING,
        StandardCopyOption.COPY_ATTRIBUTES);
  }

  @Override
  public SubscriptionSessionDataSetsHandler getSessionDataSetsHandler() {
    throw new SubscriptionIncompatibleHandlerException(
        "SubscriptionFileHandler do not support getSessionDataSetsHandler().");
  }
}
