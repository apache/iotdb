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

package org.apache.iotdb.confignode.service;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.confignode.conf.ConfigNodeRemoveCheck;

import junit.framework.TestCase;
import org.junit.Assert;
import org.mockito.Mockito;

public class ConfigNodeCommandLineTest extends TestCase {

  public void testTestRunNullArgs() throws Exception {
    ConfigNode configNodeMock = Mockito.mock(ConfigNode.class);
    ConfigNodeRemoveCheck nodeRemoveCheckMock = Mockito.mock(ConfigNodeRemoveCheck.class);

    ConfigNodeCommandLine sut = new ConfigNodeCommandLine(configNodeMock, nodeRemoveCheckMock);

    int result = sut.run(null);

    Assert.assertEquals(-1, result);
    Mockito.verify(configNodeMock, Mockito.times(0)).active();
    Mockito.verify(nodeRemoveCheckMock, Mockito.times(0)).removeCheck(Mockito.anyString());
    Mockito.verify(nodeRemoveCheckMock, Mockito.times(0))
        .removeConfigNode(Mockito.any(TConfigNodeLocation.class));
  }

  public void testTestRunNoArgs() throws Exception {
    ConfigNode configNodeMock = Mockito.mock(ConfigNode.class);
    ConfigNodeRemoveCheck nodeRemoveCheckMock = Mockito.mock(ConfigNodeRemoveCheck.class);

    ConfigNodeCommandLine sut = new ConfigNodeCommandLine(configNodeMock, nodeRemoveCheckMock);

    int result = sut.run(new String[0]);

    Assert.assertEquals(-1, result);
    Mockito.verify(configNodeMock, Mockito.times(0)).active();
    Mockito.verify(nodeRemoveCheckMock, Mockito.times(0)).removeCheck(Mockito.anyString());
    Mockito.verify(nodeRemoveCheckMock, Mockito.times(0))
        .removeConfigNode(Mockito.any(TConfigNodeLocation.class));
  }

  public void testTestRunStart() throws Exception {
    ConfigNode configNodeMock = Mockito.mock(ConfigNode.class);
    ConfigNodeRemoveCheck nodeRemoveCheckMock = Mockito.mock(ConfigNodeRemoveCheck.class);

    ConfigNodeCommandLine sut = new ConfigNodeCommandLine(configNodeMock, nodeRemoveCheckMock);

    int result = sut.run(new String[] {"-s"});

    Assert.assertEquals(0, result);
    Mockito.verify(configNodeMock, Mockito.times(1)).active();
    Mockito.verify(nodeRemoveCheckMock, Mockito.times(0)).removeCheck(Mockito.anyString());
    Mockito.verify(nodeRemoveCheckMock, Mockito.times(0))
        .removeConfigNode(Mockito.any(TConfigNodeLocation.class));
  }

  public void testTestRunRemoveNoCoordinates() throws Exception {
    ConfigNode configNodeMock = Mockito.mock(ConfigNode.class);
    ConfigNodeRemoveCheck nodeRemoveCheckMock = Mockito.mock(ConfigNodeRemoveCheck.class);

    ConfigNodeCommandLine sut = new ConfigNodeCommandLine(configNodeMock, nodeRemoveCheckMock);

    int result = sut.run(new String[] {"-r"});

    Assert.assertEquals(-1, result);
    Mockito.verify(configNodeMock, Mockito.times(0)).active();
    Mockito.verify(nodeRemoveCheckMock, Mockito.times(0)).removeCheck(Mockito.anyString());
    Mockito.verify(nodeRemoveCheckMock, Mockito.times(0))
        .removeConfigNode(Mockito.any(TConfigNodeLocation.class));
  }

  public void testTestRunRemoveValidIdCoordinates() throws Exception {
    ConfigNode configNodeMock = Mockito.mock(ConfigNode.class);
    ConfigNodeRemoveCheck nodeRemoveCheckMock = Mockito.mock(ConfigNodeRemoveCheck.class);
    TEndPoint internalEndpoint = Mockito.mock(TEndPoint.class);
    TEndPoint externalEndpoint = Mockito.mock(TEndPoint.class);
    Mockito.when(nodeRemoveCheckMock.removeCheck("23")).thenReturn(null);
    Mockito.when(nodeRemoveCheckMock.removeCheck("42"))
        .thenReturn(new TConfigNodeLocation(42, internalEndpoint, externalEndpoint));

    ConfigNodeCommandLine sut = new ConfigNodeCommandLine(configNodeMock, nodeRemoveCheckMock);

    int result = sut.run(new String[] {"-r", "42"});

    Assert.assertEquals(0, result);
    Mockito.verify(configNodeMock, Mockito.times(0)).active();
    Mockito.verify(nodeRemoveCheckMock, Mockito.times(1)).removeCheck(Mockito.anyString());
    Mockito.verify(nodeRemoveCheckMock, Mockito.times(1))
        .removeConfigNode(Mockito.any(TConfigNodeLocation.class));
  }

  public void testTestRunRemoveInvalidIdCoordinates() throws Exception {
    ConfigNode configNodeMock = Mockito.mock(ConfigNode.class);
    ConfigNodeRemoveCheck nodeRemoveCheckMock = Mockito.mock(ConfigNodeRemoveCheck.class);
    TEndPoint internalEndpoint = Mockito.mock(TEndPoint.class);
    TEndPoint externalEndpoint = Mockito.mock(TEndPoint.class);
    Mockito.when(nodeRemoveCheckMock.removeCheck("23")).thenReturn(null);
    Mockito.when(nodeRemoveCheckMock.removeCheck("42"))
        .thenReturn(new TConfigNodeLocation(42, internalEndpoint, externalEndpoint));

    ConfigNodeCommandLine sut = new ConfigNodeCommandLine(configNodeMock, nodeRemoveCheckMock);

    int result = sut.run(new String[] {"-r", "23"});

    Assert.assertEquals(-1, result);
    Mockito.verify(configNodeMock, Mockito.times(0)).active();
    Mockito.verify(nodeRemoveCheckMock, Mockito.times(1)).removeCheck(Mockito.anyString());
    Mockito.verify(nodeRemoveCheckMock, Mockito.times(0))
        .removeConfigNode(Mockito.any(TConfigNodeLocation.class));
  }

  public void testTestRunRemoveRpcCoordinates() throws Exception {
    ConfigNode configNodeMock = Mockito.mock(ConfigNode.class);
    ConfigNodeRemoveCheck nodeRemoveCheckMock = Mockito.mock(ConfigNodeRemoveCheck.class);

    ConfigNodeCommandLine sut = new ConfigNodeCommandLine(configNodeMock, nodeRemoveCheckMock);

    int result = sut.run(new String[] {"-r", "1.2.3.4:6667"});

    Assert.assertEquals(-1, result);
    Mockito.verify(configNodeMock, Mockito.times(0)).active();
    Mockito.verify(nodeRemoveCheckMock, Mockito.times(1)).removeCheck(Mockito.anyString());
    Mockito.verify(nodeRemoveCheckMock, Mockito.times(0))
        .removeConfigNode(Mockito.any(TConfigNodeLocation.class));
  }

  public void testTestRunRemoveTooManyCoordinates() throws Exception {
    ConfigNode configNodeMock = Mockito.mock(ConfigNode.class);
    ConfigNodeRemoveCheck nodeRemoveCheckMock = Mockito.mock(ConfigNodeRemoveCheck.class);

    ConfigNodeCommandLine sut = new ConfigNodeCommandLine(configNodeMock, nodeRemoveCheckMock);

    int result = sut.run(new String[] {"-r", "43", "-s"});

    Assert.assertEquals(-1, result);
    Mockito.verify(configNodeMock, Mockito.times(0)).active();
    Mockito.verify(nodeRemoveCheckMock, Mockito.times(0)).removeCheck(Mockito.anyString());
    Mockito.verify(nodeRemoveCheckMock, Mockito.times(0))
        .removeConfigNode(Mockito.any(TConfigNodeLocation.class));
  }

  public void testTestRunInvalidCommand() throws Exception {
    ConfigNode configNodeMock = Mockito.mock(ConfigNode.class);
    ConfigNodeRemoveCheck nodeRemoveCheckMock = Mockito.mock(ConfigNodeRemoveCheck.class);

    ConfigNodeCommandLine sut = new ConfigNodeCommandLine(configNodeMock, nodeRemoveCheckMock);

    int result = sut.run(new String[] {"-w"});

    Assert.assertEquals(-1, result);
    Mockito.verify(configNodeMock, Mockito.times(0)).active();
    Mockito.verify(nodeRemoveCheckMock, Mockito.times(0)).removeCheck(Mockito.anyString());
    Mockito.verify(nodeRemoveCheckMock, Mockito.times(0))
        .removeConfigNode(Mockito.any(TConfigNodeLocation.class));
  }
}
