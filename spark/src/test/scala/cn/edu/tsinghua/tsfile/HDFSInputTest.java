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
package cn.edu.tsinghua.tsfile;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.iotdb.tsfile.io.HDFSInput;
import org.apache.iotdb.tsfile.tool.TsFileWrite;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class HDFSInputTest {

  private String path = "../spark/src/test/resources/tsfile/test.tsfile";
  private HDFSInput in;

  @Before
  public void before() throws Exception {
    new TsFileWrite().create1(path);
    in = new HDFSInput(path);
  }

  @After
  public void after() throws IOException {
    in.close();
    File file = new File(path);
    file.delete();
  }

  @Test
  public void test_read1() throws IOException {
    int size = 2000;
    ByteBuffer buffer = ByteBuffer.allocate(size);
    Assert.assertEquals(size, in.read(buffer));
  }

  @Test
  public void test_read2() throws IOException {
    int size = 2000;
    long pos = 20L;
    ByteBuffer buffer = ByteBuffer.allocate(size);
    Assert.assertEquals(size, in.read(buffer, pos));
  }
}
