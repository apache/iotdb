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
package org.apache.iotdb.tsfile.common.conf;

import org.apache.iotdb.tsfile.common.constant.TsFileConstant;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class TSFileDescriptorTest {

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  private TSFileConfig tsFileConfig1;
  private TSFileConfig tsFileConfig2;

  @BeforeClass
  public static void setUpClass() {
    // System.setProperty(TsFileConstant.TSFILE_CONF, configDirectory); in the setUp method may
    // cause the other tests getting a polluted TSFileDescriptor instance in a concurrent
    // environment.
    TSFileDescriptor.getInstance();
  }

  @Before
  public void setUp() throws IOException {
    File file = tempFolder.newFile(TSFileConfig.CONFIG_FILE_NAME);
    try (FileWriter writer = new FileWriter(file)) {
      writer.write("page_size_in_byte=1234\n");
      writer.write("max_string_length=bad\n");
      writer.write("float_precision=4\n");
      writer.write("batch_size=100\n");
    }
    String configDirectory = tempFolder.getRoot().getAbsolutePath();

    String originalProperty = System.getProperty(TsFileConstant.TSFILE_CONF);
    System.setProperty(TsFileConstant.TSFILE_CONF, configDirectory);
    this.tsFileConfig1 = new TSFileConfig();
    this.tsFileConfig2 = new TSFileDescriptor().getConfig();
    if (originalProperty == null) {
      System.clearProperty(TsFileConstant.TSFILE_CONF);
    } else {
      System.setProperty(TsFileConstant.TSFILE_CONF, originalProperty);
    }
  }

  @Test
  public void testGetInstanceOverWriteFromFile() {

    Assert.assertEquals(65536, tsFileConfig1.getPageSizeInByte());
    Assert.assertEquals(1234, tsFileConfig2.getPageSizeInByte());

    Assert.assertEquals(2, tsFileConfig1.getFloatPrecision());
    Assert.assertEquals(4, tsFileConfig2.getFloatPrecision());

    Assert.assertEquals(1000, tsFileConfig1.getBatchSize());
    Assert.assertEquals(100, tsFileConfig2.getBatchSize());

    Assert.assertEquals(tsFileConfig1.getMaxStringLength(), tsFileConfig2.getMaxStringLength());
  }
}
