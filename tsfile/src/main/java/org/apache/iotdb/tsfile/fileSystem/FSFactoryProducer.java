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

package org.apache.iotdb.tsfile.fileSystem;

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.fileSystem.fileInputFactory.FileInputFactory;
import org.apache.iotdb.tsfile.fileSystem.fileInputFactory.HDFSInputFactory;
import org.apache.iotdb.tsfile.fileSystem.fileInputFactory.LocalFSInputFactory;
import org.apache.iotdb.tsfile.fileSystem.fileOutputFactory.FileOutputFactory;
import org.apache.iotdb.tsfile.fileSystem.fileOutputFactory.HDFSOutputFactory;
import org.apache.iotdb.tsfile.fileSystem.fileOutputFactory.LocalFSOutputFactory;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.HDFSFactory;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.LocalFSFactory;

public class FSFactoryProducer {

  private static FSType fSType = TSFileDescriptor.getInstance().getConfig().getTSFileStorageFs();

  private static FSFactory fsFactory;
  private static FileInputFactory fileInputFactory;
  private static FileOutputFactory fileOutputFactory;

  static {
    if (fSType.equals(FSType.HDFS)) {
      fsFactory = new HDFSFactory();
      fileInputFactory = new HDFSInputFactory();
      fileOutputFactory = new HDFSOutputFactory();
    } else {
      fsFactory = new LocalFSFactory();
      fileInputFactory = new LocalFSInputFactory();
      fileOutputFactory = new LocalFSOutputFactory();
    }
  }

  public static FSFactory getFSFactory() {
    return fsFactory;
  }

  public static FileInputFactory getFileInputFactory() {
    return fileInputFactory;
  }

  public static FileOutputFactory getFileOutputFactory() {
    return fileOutputFactory;
  }
}
