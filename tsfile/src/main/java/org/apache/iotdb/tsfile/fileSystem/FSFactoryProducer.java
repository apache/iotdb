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

import org.apache.iotdb.tsfile.fileSystem.fileInputFactory.FileInputFactory;
import org.apache.iotdb.tsfile.fileSystem.fileInputFactory.HybridFileInputFactory;
import org.apache.iotdb.tsfile.fileSystem.fileOutputFactory.FileOutputFactory;
import org.apache.iotdb.tsfile.fileSystem.fileOutputFactory.HybridFileOutputFactory;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.HybridFSFactory;

public class FSFactoryProducer {
  private static FSFactory fsFactory = new HybridFSFactory();
  private static FileInputFactory fileInputFactory = new HybridFileInputFactory();
  private static FileOutputFactory fileOutputFactory = new HybridFileOutputFactory();

  public static FSFactory getFSFactory() {
    return fsFactory;
  }

  public static FileInputFactory getFileInputFactory() {
    return fileInputFactory;
  }

  public static void setFsFactory(FSFactory fsFactory) {
    FSFactoryProducer.fsFactory = fsFactory;
  }

  public static void setFileInputFactory(FileInputFactory fileInputFactory) {
    FSFactoryProducer.fileInputFactory = fileInputFactory;
  }

  public static FileOutputFactory getFileOutputFactory() {
    return fileOutputFactory;
  }

  public static void setFileOutputFactory(FileOutputFactory fileOutputFactory) {
    FSFactoryProducer.fileOutputFactory = fileOutputFactory;
  }
}
