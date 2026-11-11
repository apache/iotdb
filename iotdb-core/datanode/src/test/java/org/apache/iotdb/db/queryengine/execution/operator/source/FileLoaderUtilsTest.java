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

package org.apache.iotdb.db.queryengine.execution.operator.source;

import org.apache.iotdb.commons.path.AlignedFullPath;
import org.apache.iotdb.db.exception.CorruptedTsFileException;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.storageengine.buffer.TimeSeriesMetadataCache;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileID;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.IDeviceID.Factory;
import org.apache.tsfile.read.filter.basic.Filter;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FileLoaderUtilsTest {

  @Test
  public void testLoadAlignedTimeSeriesMetadataBatchAbsentDevice() throws Exception {
    TsFileResource resource = Mockito.mock(TsFileResource.class);
    Mockito.when(resource.getTsFileID()).thenReturn(new TsFileID());
    Mockito.when(resource.getTsFile()).thenReturn(new File("target/x.tsfile"));

    TimeSeriesMetadataCache cache = Mockito.mock(TimeSeriesMetadataCache.class);
    Mockito.when(
            cache.getDeviceTimeSeriesMetadata(
                Mockito.anyString(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.anyBoolean()))
        .thenReturn(Collections.emptyMap());

    IDeviceID deviceId = Factory.DEFAULT_FACTORY.create("root.device");

    Method method =
        FileLoaderUtils.class.getDeclaredMethod(
            "loadAlignedTimeSeriesMetadataBatch",
            TsFileResource.class,
            AlignedFullPath.class,
            List.class,
            FragmentInstanceContext.class,
            Filter.class,
            boolean.class,
            boolean.class,
            TimeSeriesMetadataCache.class,
            String.class,
            IDeviceID.class);
    method.setAccessible(true);

    // a device absent from a device-level time index file is an inconsistency: surface it
    try {
      method.invoke(
          null,
          resource,
          null,
          Collections.emptyList(),
          null,
          null,
          false,
          false,
          cache,
          "target/x.tsfile",
          deviceId);
      fail("expected CorruptedTsFileException when the device is absent and ignoreNotExists=false");
    } catch (InvocationTargetException e) {
      assertTrue(e.getCause() instanceof CorruptedTsFileException);
    }

    // with a file-level time index (or during recovery) an absent device is expected: skip it
    try {
      Object result =
          method.invoke(
              null,
              resource,
              null,
              Collections.emptyList(),
              null,
              null,
              false,
              true,
              cache,
              "target/x.tsfile",
              deviceId);
      assertNull(result);
    } catch (InvocationTargetException e) {
      fail("unexpected exception: " + e.getCause());
    }
  }
}
