/**
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
package org.apache.iotdb.db.engine.overflow.ioV2;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.iotdb.tsfile.read.reader.TsFileInput;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class OverflowIOTest {

  private String overflowFilePath = "overflowfile";
  private OverflowIO io = null;
  private TsFileInput reader = null;

  @Before
  public void setUp() throws Exception {
    io = new OverflowIO(new OverflowIO.OverflowReadWriter(overflowFilePath));
    reader = new OverflowIO.OverflowReadWriter(overflowFilePath);
  }

  @After
  public void tearDown() throws Exception {
    io.close();
    reader.close();
    File file = new File(overflowFilePath);
    file.delete();
  }

  @Test
  public void testFileCutoff() throws IOException {
    File file = new File("testoverflowfile");
    FileOutputStream fileOutputStream = new FileOutputStream(file);
    byte[] bytes = new byte[20];
    fileOutputStream.write(bytes);
    fileOutputStream.close();
    assertEquals(20, file.length());
    OverflowIO overflowIO = new OverflowIO(new OverflowIO.OverflowReadWriter(file.getPath()));
    assertEquals(20, file.length());
    overflowIO.close();
    file.delete();
  }

}
