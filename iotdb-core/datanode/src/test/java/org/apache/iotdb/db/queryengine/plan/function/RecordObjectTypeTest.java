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

package org.apache.iotdb.db.queryengine.plan.function;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.queryengine.execution.operator.process.function.partition.Slice;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.RecordIterator;
import org.apache.iotdb.db.storageengine.rescon.disk.TierManager;
import org.apache.iotdb.udf.api.relational.access.Record;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.tsfile.block.TsBlockBuilderStatus;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilderStatus;
import org.apache.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.apache.tsfile.read.common.type.ObjectType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;

import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.RecordIterator.OBJECT_ERR_MSG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RecordObjectTypeTest {

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
  public void test() throws IOException {
    BinaryColumnBuilder columnBuilder =
        new BinaryColumnBuilder(new ColumnBuilderStatus(new TsBlockBuilderStatus(1024)), 1);
    columnBuilder.writeBinary(new Binary(createObjectBinary().array()));
    RecordIterator recordIterator =
        new RecordIterator(
            Collections.singletonList(columnBuilder.build()),
            Collections.singletonList(ObjectType.OBJECT),
            1);
    Slice slice =
        new Slice(
            0,
            1,
            new Column[] {columnBuilder.build()},
            Collections.singletonList(0),
            Collections.emptyList(),
            Collections.singletonList(Type.OBJECT));
    testRecordIterator(recordIterator);
    testRecordIterator(slice.getRequiredRecordIterator(false));
  }

  private void testRecordIterator(Iterator<Record> recordIterator) {
    Assert.assertTrue(recordIterator.hasNext());
    Record record = recordIterator.next();

    Binary result = record.readObject(0);
    assertEquals(100, result.getLength());
    for (int j = 0; j < 100; j++) {
      assertEquals(j, result.getValues()[j]);
    }

    result = record.readObject(0, 10, 2);
    Assert.assertArrayEquals(new byte[] {(byte) 10, (byte) 11}, result.getValues());

    try {
      record.getObject(0);
      fail("Should throw exception");
    } catch (UnsupportedOperationException e) {
      assertEquals(OBJECT_ERR_MSG, e.getMessage());
    }

    try {
      record.getBinary(0);
      fail("Should throw exception");
    } catch (UnsupportedOperationException e) {
      assertEquals(OBJECT_ERR_MSG, e.getMessage());
    }

    Optional<File> objectFile = record.getObjectFile(0);
    assertTrue(objectFile.isPresent());

    assertEquals("(Object) 100 B", record.getString(0));
    Assert.assertFalse(recordIterator.hasNext());
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
}
