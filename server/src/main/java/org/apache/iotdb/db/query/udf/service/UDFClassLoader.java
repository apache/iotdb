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

package org.apache.iotdb.db.query.udf.service;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;

public class UDFClassLoader extends URLClassLoader {

  private final String libRoot;
  private final Set<File> fileSet;

  public UDFClassLoader(String libRoot) throws IOException {
    super(new URL[0]);
    this.libRoot = libRoot;
    fileSet = new HashSet<>();
    refresh();
  }

  public void refresh() throws IOException {
    URL[] urls = collectNewURLsFromLibRoot();
    for (URL url : urls) {
      super.addURL(url);
    }
  }

  private synchronized URL[] collectNewURLsFromLibRoot() throws IOException {
    HashSet<File> newFileSet = new HashSet<>(
        FileUtils.listFiles(SystemFileFactory.INSTANCE.getFile(libRoot), null, true));
    newFileSet.removeAll(fileSet);
    URL[] urls = FileUtils.toURLs(newFileSet.toArray(new File[0]));
    fileSet.addAll(newFileSet);
    return urls;
  }
}
