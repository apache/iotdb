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
package org.apache.iotdb.db.metadata.schemaregion.rocksdb;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.schemaregion.rocksdb.mnode.RMNodeType;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.metadata.schemaregion.rocksdb.RSchemaReadWriteHandler.ROCKSDB_PATH;

@Ignore
public class RSchemaReadWriteHandlerTest {

  private RSchemaReadWriteHandler readWriteHandler;

  @Before
  public void setUp() throws MetadataException, RocksDBException {
    File file = new File(ROCKSDB_PATH);
    if (!file.exists()) {
      file.mkdirs();
    }
    readWriteHandler = new RSchemaReadWriteHandler();
  }

  @Test
  public void testKeyExistByTypes() throws IllegalPathException, RocksDBException {
    List<PartialPath> timeseries = new ArrayList<>();
    timeseries.add(new PartialPath("root.sg.d1.m1"));
    timeseries.add(new PartialPath("root.sg.d1.m2"));
    timeseries.add(new PartialPath("root.sg.d2.m1"));
    timeseries.add(new PartialPath("root.sg.d2.m2"));
    timeseries.add(new PartialPath("root.sg1.d1.m1"));
    timeseries.add(new PartialPath("root.sg1.d1.m2"));
    timeseries.add(new PartialPath("root.sg1.d2.m1"));
    timeseries.add(new PartialPath("root.sg1.d2.m2"));

    for (PartialPath path : timeseries) {
      String levelPath = RSchemaUtils.getLevelPath(path.getNodes(), path.getNodeLength() - 1);
      readWriteHandler.createNode(levelPath, RMNodeType.MEASUREMENT, path.getFullPath().getBytes());
    }

    for (PartialPath path : timeseries) {
      String levelPath = RSchemaUtils.getLevelPath(path.getNodes(), path.getNodeLength() - 1);
      CheckKeyResult result = readWriteHandler.keyExistByAllTypes(levelPath);
      Assert.assertTrue(result.existAnyKey());
      Assert.assertNotNull(result.getValue());
      Assert.assertEquals(path.getFullPath(), new String(result.getValue()));
    }
  }
}
