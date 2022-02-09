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

package org.apache.iotdb.db.engine.trigger.service;

import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashSet;

public class TriggerClassLoader extends URLClassLoader {

  private static final Logger LOGGER = LoggerFactory.getLogger(TriggerClassLoader.class);

  private final String libRoot;

  TriggerClassLoader(String libRoot) throws IOException {
    super(new URL[0]);
    this.libRoot = libRoot;
    LOGGER.info("Trigger lib root: {}", libRoot);
    addURLs();
  }

  private void addURLs() throws IOException {
    HashSet<File> fileSet =
        new HashSet<>(FileUtils.listFiles(SystemFileFactory.INSTANCE.getFile(libRoot), null, true));
    URL[] urls = FileUtils.toURLs(fileSet.toArray(new File[0]));
    for (URL url : urls) {
      super.addURL(url);
    }
  }
}
