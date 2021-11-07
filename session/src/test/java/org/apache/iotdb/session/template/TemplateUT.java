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
package org.apache.iotdb.session.template;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class TemplateUT {

  @Before
  public void setUp() throws Exception {
    System.setProperty(IoTDBConstant.IOTDB_CONF, "src/test/resources/");
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testTemplateTree() {
    Template template = new Template("treeTemplate", true);
    Node iNodeGPS = new InternalNode("GPS", false);
    Node iNodeV = new InternalNode("vehicle", true);
    Node mNodeX =
        new MeasurementNode("x", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY);
    Node mNodeY =
        new MeasurementNode("y", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY);
    ByteArrayOutputStream stream = new ByteArrayOutputStream();

    try {
      iNodeGPS.addChild(mNodeX);
      iNodeGPS.addChild(mNodeY);
      iNodeV.addChild(mNodeX);
      iNodeV.addChild(mNodeY);
      iNodeV.addChild(iNodeGPS);
      template.addToTemplate(iNodeGPS);
      template.addToTemplate(iNodeV);
      template.addToTemplate(mNodeX);
      template.addToTemplate(mNodeY);


      template.serialize(stream);
      ByteBuffer buffer = ByteBuffer.wrap(stream.toByteArray());

      assertEquals("treeTemplate", ReadWriteIOUtils.readString(buffer));


    } catch (StatementExecutionException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
