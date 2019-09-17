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
import org.apache.iotdb.tsfile.fileSystem.HDFSOutput;
import org.apache.iotdb.tsfile.hadoop.io.HDFSInput;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

/**
 * Test the {@link org.apache.iotdb.tsfile.hadoop.io.HDFSInput}
 *
 * @author Yuan Tian
 */
public class HDFSInputAndOutputTest {

  private int lenOfBytes = 50;
  private byte[] bs = new byte[lenOfBytes];
  private byte[] rbs = new byte[lenOfBytes];
  private ByteBuffer buffer;
  private ByteBuffer rBuffer = ByteBuffer.allocate(lenOfBytes);
  private String filename = "testHDFSInputAndOutput.tsfile";
  private Charset charset = StandardCharsets.UTF_8;
  private Path path;
  private FileSystem fileSystem;

  @Before
  public void setUp() throws Exception {
    fileSystem = FileSystem.get(new Configuration());
    path = new Path(filename);
    fileSystem.delete(path, true);
    // initialize the bytes array and byteBuffer
    byte[] temp = filename.getBytes();
    int len = temp.length;
    for (int i = 0; i < lenOfBytes; i++) {
      bs[i] = temp[i % len];
    }
    buffer = ByteBuffer.wrap(bs);
  }

  @After
  public void tearDown() throws Exception {
    if (fileSystem.exists(path)) {
      fileSystem.delete(path, true);
    }
  }

  @Test
  public void testBytesArrayReadAndWrite() throws Exception {
    HDFSOutput hdfsOutput = new HDFSOutput(filename, new Configuration(), true);
    hdfsOutput.write(bs);
    assertEquals(lenOfBytes, hdfsOutput.getPosition());
    hdfsOutput.close();
    assertTrue(fileSystem.exists(path));

    // read bytes using hdfs inputStream
    HDFSInput hdfsInput = new HDFSInput(filename);
    assertEquals(0, hdfsInput.position());
    assertEquals(lenOfBytes, hdfsInput.size());
    hdfsInput.position(10);
    assertEquals(10, hdfsInput.position());
    // verify that this read method does not modify the position
    hdfsInput.read(rbs, 0, rbs.length);
    assertEquals(10, hdfsInput.position());
    assertArrayEquals(bs, rbs);


    hdfsInput.close();
    assertTrue(fileSystem.exists(path));
    fileSystem.delete(path, true);
    assertFalse(fileSystem.exists(path));
  }

  @Test
  public void testByteBufferReadAndWrite() throws Exception {
    // write one byte array
    HDFSOutput hdfsOutput = new HDFSOutput(filename, new Configuration(), true);
    hdfsOutput.write(buffer);
    assertEquals(lenOfBytes, hdfsOutput.getPosition());
    hdfsOutput.close();
    assertTrue(fileSystem.exists(path));

    // read bytes using hdfs inputStream
    HDFSInput hdfsInput = new HDFSInput(filename);
    assertEquals(0, hdfsInput.position());
    assertEquals(lenOfBytes, hdfsInput.size());
    hdfsInput.position(10);
    assertEquals(10, hdfsInput.position());

    hdfsInput.position(0);
    // test the read method without position
    hdfsInput.read(rBuffer);
    // verify that this read method modify the position
    assertEquals(lenOfBytes, hdfsInput.position());
    buffer.position(0);
    buffer.limit(lenOfBytes);
    rBuffer.position(0);
    rBuffer.limit(lenOfBytes);
    assertEquals(charset.decode(buffer).toString(), charset.decode(rBuffer).toString());

    // test the read method with position
    rBuffer.clear();
    hdfsInput.read(rBuffer, 0);
    // verify that this read method does not modify the position
    assertEquals(lenOfBytes, hdfsInput.position());
    buffer.position(0);
    buffer.limit(lenOfBytes);
    rBuffer.position(0);
    rBuffer.limit(lenOfBytes);
    assertEquals(charset.decode(buffer).toString(), charset.decode(rBuffer).toString());

    hdfsInput.close();
    assertTrue(fileSystem.exists(path));
    fileSystem.delete(path, true);
    assertFalse(fileSystem.exists(path));
  }

}