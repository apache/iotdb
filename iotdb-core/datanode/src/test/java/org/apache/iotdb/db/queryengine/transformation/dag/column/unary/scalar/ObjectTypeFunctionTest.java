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

package org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.storageengine.rescon.disk.TierManager;

import org.apache.tsfile.block.TsBlockBuilderStatus;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilderStatus;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.read.common.type.BlobType;
import org.apache.tsfile.read.common.type.LongType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

public class ObjectTypeFunctionTest {

  private final CommonConfig config = CommonDescriptor.getInstance().getConfig();

  private File objectDir;

  @Before
  public void setup() {
    config.setRestrictObjectLimit(true);
    try {
      objectDir = new File(TierManager.getInstance().getNextFolderForObjectFile());
    } catch (DiskSpaceInsufficientException e) {
      throw new RuntimeException(e);
    }
  }

  @After
  public void tearDown() throws IOException {
    File[] files = objectDir.listFiles();
    if (files != null) {
      for (File file : files) {
        Files.delete(file.toPath());
      }
    }
    config.setRestrictObjectLimit(false);
  }

  @Test
  public void testLength() throws IOException {
    BinaryColumnBuilder columnBuilder =
        new BinaryColumnBuilder(new ColumnBuilderStatus(new TsBlockBuilderStatus(1024)), 1);
    columnBuilder.writeBinary(new Binary(createObjectBinary().array()));
    ColumnTransformer childColumnTransformer = mockChildColumnTransformer(columnBuilder.build());
    ObjectLengthColumnTransformer transformer =
        new ObjectLengthColumnTransformer(LongType.INT64, childColumnTransformer);
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();
    Assert.assertTrue(result instanceof LongColumn);
    Assert.assertEquals(LongType.INT64, transformer.getType());
    Assert.assertEquals(1, result.getPositionCount());
    for (int i = 0; i < result.getPositionCount(); i++) {
      Assert.assertEquals(100, result.getLong(i));
    }
  }

  @Test
  public void testReadObject1() throws IOException {
    BinaryColumnBuilder columnBuilder =
        new BinaryColumnBuilder(new ColumnBuilderStatus(new TsBlockBuilderStatus(1024)), 1);
    columnBuilder.writeBinary(new Binary(createObjectBinary().array()));
    ColumnTransformer childColumnTransformer = mockChildColumnTransformer(columnBuilder.build());
    ReadObjectColumnTransformer transformer =
        new ReadObjectColumnTransformer(BlobType.BLOB, childColumnTransformer, Optional.empty());
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();
    Assert.assertTrue(result instanceof BinaryColumn);
    Assert.assertEquals(BlobType.BLOB, transformer.getType());
    Assert.assertEquals(1, result.getPositionCount());
    for (int i = 0; i < result.getPositionCount(); i++) {
      Assert.assertEquals(100, result.getBinary(i).getLength());
      for (int j = 0; j < 100; j++) {
        Assert.assertEquals(j, result.getBinary(i).getValues()[j]);
      }
    }
  }

  @Test
  public void testReadObject2() throws IOException {
    BinaryColumnBuilder columnBuilder =
        new BinaryColumnBuilder(new ColumnBuilderStatus(new TsBlockBuilderStatus(1024)), 1);
    columnBuilder.writeBinary(new Binary(createObjectBinary().array()));
    ColumnTransformer childColumnTransformer = mockChildColumnTransformer(columnBuilder.build());
    ReadObjectColumnTransformer transformer =
        new ReadObjectColumnTransformer(
            BlobType.BLOB, 10, childColumnTransformer, Optional.empty());
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();
    Assert.assertTrue(result instanceof BinaryColumn);
    Assert.assertEquals(BlobType.BLOB, transformer.getType());
    Assert.assertEquals(1, result.getPositionCount());
    for (int i = 0; i < result.getPositionCount(); i++) {
      Assert.assertEquals(90, result.getBinary(i).getLength());
      for (int j = 10; j < 100; j++) {
        Assert.assertEquals(j, result.getBinary(i).getValues()[j - 10]);
      }
    }
  }

  @Test
  public void testReadObject3() throws IOException {
    BinaryColumnBuilder columnBuilder =
        new BinaryColumnBuilder(new ColumnBuilderStatus(new TsBlockBuilderStatus(1024)), 1);
    columnBuilder.writeBinary(new Binary(createObjectBinary().array()));
    ColumnTransformer childColumnTransformer = mockChildColumnTransformer(columnBuilder.build());
    ReadObjectColumnTransformer transformer =
        new ReadObjectColumnTransformer(
            BlobType.BLOB, 10, 2, childColumnTransformer, Optional.empty());
    transformer.addReferenceCount();
    transformer.evaluate();
    Column result = transformer.getColumn();
    Assert.assertTrue(result instanceof BinaryColumn);
    Assert.assertEquals(BlobType.BLOB, transformer.getType());
    Assert.assertEquals(1, result.getPositionCount());
    for (int i = 0; i < result.getPositionCount(); i++) {
      Assert.assertEquals(2, result.getBinary(i).getLength());
      Assert.assertArrayEquals(new byte[] {(byte) 10, (byte) 11}, result.getBinary(i).getValues());
    }
  }

  private ByteBuffer createObjectBinary() throws IOException {
    Path testFile1 = Files.createTempFile(objectDir.toPath(), "test_", ".bin");
    byte[] content = new byte[100];
    for (int i = 0; i < 100; i++) {
      content[i] = (byte) i;
    }
    Files.write(testFile1, content);
    String relativePath = testFile1.toFile().getName();
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + relativePath.length());
    buffer.putLong(100L);
    buffer.put(BytesUtils.stringToBytes(relativePath));
    buffer.flip();
    return buffer;
  }

  private ColumnTransformer mockChildColumnTransformer(Column column) {
    ColumnTransformer mockColumnTransformer = Mockito.mock(ColumnTransformer.class);
    Mockito.when(mockColumnTransformer.getColumn()).thenReturn(column);
    Mockito.doNothing().when(mockColumnTransformer).tryEvaluate();
    Mockito.doNothing().when(mockColumnTransformer).clearCache();
    Mockito.doNothing().when(mockColumnTransformer).evaluateWithSelection(Mockito.any());
    return mockColumnTransformer;
  }
}
