/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.engine.fileSystem;

import org.apache.iotdb.db.conf.IoTDBDescriptor;

import java.io.File;
import java.net.URI;

public enum FileFactory {

  INSTANCE;

  private static FSType FSType = IoTDBDescriptor.getInstance().getConfig().getStorageFs();

  public File getFile(String pathname) {
    if (FSType.equals(FSType.HDFS)) {
      return new HdfsFile(pathname);
    } else {
      return new File(pathname);
    }
  }

  public File getFile(String parent, String child) {
    if (FSType.equals(FSType.HDFS)) {
      return new HdfsFile(parent, child);
    } else {
      return new File(parent, child);
    }
  }

  public File getFile(File parent, String child) {
    if (FSType.equals(FSType.HDFS)) {
      return new HdfsFile(parent, child);
    } else {
      return new File(parent, child);
    }
  }

  public File getFile(URI uri) {
    if (FSType.equals(FSType.HDFS)) {
      return new HdfsFile(uri);
    } else {
      return new File(uri);
    }
  }

}