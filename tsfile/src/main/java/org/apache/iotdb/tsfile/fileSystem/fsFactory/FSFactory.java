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

package org.apache.iotdb.tsfile.fileSystem.fsFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.net.URI;

public interface FSFactory {

  File getFile(String pathname);

  File getFile(String parent, String child);

  File getFile(File parent, String child);

  File getFile(URI uri);

  BufferedReader getBufferedReader(String filePath);

  BufferedWriter getBufferedWriter(String filePath, boolean append);

  BufferedInputStream getBufferedInputStream(String filePath);

  BufferedOutputStream getBufferedOutputStream(String filePath);

  void moveFile(File srcFile, File destFile);

  File[] listFilesBySuffix(String fileFolder, String suffix);

  File[] listFilesByPrefix(String fileFolder, String prefix);
}