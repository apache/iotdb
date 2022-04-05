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
package org.apache.iotdb.db.mpp.operator;

import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.GroupType;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.execution.FragmentInstanceContext;
import org.apache.iotdb.db.mpp.operator.meta.MetaScanOperator;
import org.apache.iotdb.db.mpp.operator.meta.TimeSeriesMetaScanOperator;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.query.reader.series.SeriesReaderTestUtil;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.BinaryColumn;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_ATTRIBUTES;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_STORAGE_GROUP;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_TAGS;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_TIMESERIES;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_TIMESERIES_ALIAS;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_TIMESERIES_COMPRESSION;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_TIMESERIES_DATATYPE;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_TIMESERIES_ENCODING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MetaScanOperatorTest {
  private static final String META_SCAN_OPERATOR_TEST_SG = "root.MetaScanOperatorTest";
  private final List<String> deviceIds = new ArrayList<>();
  private final List<MeasurementSchema> measurementSchemas = new ArrayList<>();

  private final List<TsFileResource> seqResources = new ArrayList<>();
  private final List<TsFileResource> unSeqResources = new ArrayList<>();

  @Before
  public void setUp() throws MetadataException, IOException, WriteProcessException {
    SeriesReaderTestUtil.setUp(
        measurementSchemas, deviceIds, seqResources, unSeqResources, META_SCAN_OPERATOR_TEST_SG);
  }

  @After
  public void tearDown() throws IOException {
    SeriesReaderTestUtil.tearDown(seqResources, unSeqResources);
  }

  @Test
  public void TimeSeriesMetaScanOperatorTest() {
    try {
      MeasurementPath measurementPath =
          new MeasurementPath(META_SCAN_OPERATOR_TEST_SG + ".device0.sensor0", TSDataType.INT32);
      QueryId queryId = new QueryId("stub_query");
      FragmentInstanceContext fragmentInstanceContext =
          new FragmentInstanceContext(
              new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance"));
      fragmentInstanceContext.addOperatorContext(
          1, new PlanNodeId("1"), MetaScanOperator.class.getSimpleName());
      List<String> columns =
          Arrays.asList(
              COLUMN_TIMESERIES,
              COLUMN_TIMESERIES_ALIAS,
              COLUMN_STORAGE_GROUP,
              COLUMN_TIMESERIES_DATATYPE,
              COLUMN_TIMESERIES_ENCODING,
              COLUMN_TIMESERIES_COMPRESSION,
              COLUMN_TAGS,
              COLUMN_ATTRIBUTES);
      TimeSeriesMetaScanOperator timeSeriesMetaScanOperator =
          new TimeSeriesMetaScanOperator(
              fragmentInstanceContext.getOperatorContexts().get(0),
              new ConsensusGroupId(GroupType.SchemaRegion, 0),
              10,
              0,
              new PartialPath(META_SCAN_OPERATOR_TEST_SG + ".device0.*"),
              null,
              null,
              false,
              false,
              false,
              columns);
      while (timeSeriesMetaScanOperator.hasNext()) {
        TsBlock tsBlock = timeSeriesMetaScanOperator.next();
        assertEquals(8, tsBlock.getValueColumnCount());
        assertTrue(tsBlock.getColumn(0) instanceof BinaryColumn);
        assertEquals(10, tsBlock.getPositionCount());
        for (int i = 0; i < tsBlock.getPositionCount(); i++) {
          Assert.assertEquals(0, tsBlock.getTimeByIndex(i));
          for (int j = 0; j < columns.size(); j++) {
            Binary binary =
                tsBlock.getColumn(j).isNull(i) ? null : tsBlock.getColumn(j).getBinary(i);
            String value = binary == null ? "null" : binary.toString();
            switch (j) {
              case 0:
                Assert.assertTrue(value.startsWith(META_SCAN_OPERATOR_TEST_SG + ".device0"));
                break;
              case 1:
                assertEquals("null", value);
                break;
              case 2:
                assertEquals(META_SCAN_OPERATOR_TEST_SG, value);
                break;
              case 3:
                assertEquals(TSDataType.INT32.toString(), value);
                break;
              case 4:
                assertEquals(TSEncoding.PLAIN.toString(), value);
                break;
              case 5:
                assertEquals(CompressionType.UNCOMPRESSED.toString(), value);
                break;
              case 6:
              case 7:
                assertTrue(StringUtils.isBlank(value));
              default:
                break;
            }
          }
        }
      }
    } catch (IOException | IllegalPathException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testTsBlock() {
    TsBlockBuilder tsBlockBuilder =
        new TsBlockBuilder(Arrays.asList(TSDataType.TEXT, TSDataType.INT32));
    for (int i = 0; i < 2; i++) {
      tsBlockBuilder.getTimeColumnBuilder().writeLong(i);
      tsBlockBuilder.getColumnBuilder(0).appendNull();
      tsBlockBuilder.getColumnBuilder(1).writeInt(i);
      tsBlockBuilder.declarePosition();
    }
    TsBlock tsBlock = tsBlockBuilder.build();
    System.out.println(tsBlock.getColumn(0).isNull(0));
  }
}
