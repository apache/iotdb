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
package org.apache.iotdb.tsfile.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iotdb.tsfile.hadoop.io.HDFSInput;
import org.apache.iotdb.tsfile.hadoop.io.HDFSOutputStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;


public class InputOutputStreamTest {

  private HDFSInput hdfsInput = null;
  private HDFSOutputStream hdfsOutputStream = null;
  private int lenOfBytes = 50;
  private byte b = 10;
  private byte[] bs = new byte[lenOfBytes];
  private byte[] rbs = new byte[lenOfBytes];
  private String filename = "testinputandoutputstream.file";
  private Path path;
  private FileSystem fileSystem;

  @Before
  public void setUp() throws Exception {

    fileSystem = FileSystem.get(new Configuration());
    path = new Path(filename);
    fileSystem.delete(path, true);
  }

  @After
  public void tearDown() throws Exception {
    if (fileSystem.exists(path)) {
      fileSystem.delete(path, true);
    }
  }

  @Test
  public void test() throws Exception {
    // write one byte
    hdfsOutputStream = new HDFSOutputStream(filename, new Configuration(), true);
    hdfsOutputStream.write(b);
    assertEquals(1, hdfsOutputStream.getPos());
    hdfsOutputStream.close();
    assertEquals(true, fileSystem.exists(path));
    fileSystem.delete(path, true);
    assertEquals(false, fileSystem.exists(path));
    // write bytes
    hdfsOutputStream = new HDFSOutputStream(filename, new Configuration(), true);
    hdfsOutputStream.write(bs);
    assertEquals(bs.length, hdfsOutputStream.getPos());
    hdfsOutputStream.close();
    assertEquals(true, fileSystem.exists(path));
    // read bytes using hdfs inputstream
    hdfsInput = new HDFSInput(filename);
    assertEquals(0, hdfsInput.position());
    assertEquals(lenOfBytes, hdfsInput.size());
    hdfsInput.position(10);
    assertEquals(10, hdfsInput.position());
    hdfsInput.position(0);
    hdfsInput.read(rbs, 0, rbs.length);
    assertEquals(lenOfBytes, hdfsInput.position());
    assertArrayEquals(bs, rbs);
    hdfsInput.close();
    assertEquals(true, fileSystem.exists(path));
    fileSystem.delete(path, true);
    assertEquals(false, fileSystem.exists(path));
  }

}