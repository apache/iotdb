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

package org.apache.iotdb.jdbc;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class IoTDBDataSourceFactoryTest {

  private IoTDBDataSourceFactory ioTDBDataSourceFactory;

  @Mock private Properties properties;

  @Before
  public void before() throws Exception {
    MockitoAnnotations.initMocks(this);
    ioTDBDataSourceFactory = new IoTDBDataSourceFactory();
  }

  @Test
  public void createDataSource() {
    when(properties.clone()).thenReturn(properties);
    when(properties.remove(any())).thenReturn("test");
    IoTDBDataSource dataSource =
        (IoTDBDataSource) ioTDBDataSourceFactory.createDataSource(properties);
    assertEquals(dataSource.getUrl(), "test");
    assertEquals(dataSource.getPassword(), "test");
    assertEquals(dataSource.getUser(), "test");
  }

  @Test
  public void createConnectionPoolDataSource() {
    assertNull(ioTDBDataSourceFactory.createConnectionPoolDataSource(null));
  }

  @Test
  public void createXADataSource() {
    assertNull(ioTDBDataSourceFactory.createXADataSource(null));
  }

  @Test
  public void createDriver() {
    assertEquals(
        ioTDBDataSourceFactory.createDriver(null).getClass().getName(),
        new IoTDBDriver().getClass().getName());
  }
}
