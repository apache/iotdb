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
 * software distributed under this License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.pipe.sink.util;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class TabletStatementConverterTest {

  @Test
  public void testConvertStatementToTabletTreeModel() throws MetadataException {
    final int columnCount = 1000;
    final int rowCount = 100;
    final String deviceName = "root.sg.device";
    final boolean isAligned = true;

    // Generate Tablet and construct Statement from it
    final Tablet originalTablet = generateTreeModelTablet(deviceName, columnCount, rowCount);
    final InsertTabletStatement statement =
        new InsertTabletStatement(originalTablet, isAligned, null);

    // Convert Statement to Tablet
    final Tablet convertedTablet = statement.convertToTablet();

    // Verify conversion
    assertTabletsEqual(originalTablet, convertedTablet);
  }

  @Test
  public void testConvertStatementToTabletTableModel() throws MetadataException {
    final int columnCount = 1000;
    final int rowCount = 100;
    final String tableName = "table1";
    final String databaseName = "test_db";
    final boolean isAligned = false;

    // Generate Tablet and construct Statement from it
    final Tablet originalTablet = generateTableModelTablet(tableName, columnCount, rowCount);
    final InsertTabletStatement statement =
        new InsertTabletStatement(originalTablet, isAligned, databaseName);

    // Convert Statement to Tablet
    final Tablet convertedTablet = statement.convertToTablet();

    // Verify conversion
    assertTabletsEqual(originalTablet, convertedTablet);
  }

  @Test
  public void testDeserializeStatementFromTabletFormat() throws IOException, MetadataException {
    final int columnCount = 1000;
    final int rowCount = 100;
    final String deviceName = "root.sg.device";

    // Generate test Tablet
    final Tablet originalTablet = generateTreeModelTablet(deviceName, columnCount, rowCount);

    // Serialize Tablet to ByteBuffer
    final PublicBAOS byteArrayOutputStream = new PublicBAOS();
    final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
    // Then serialize the tablet
    originalTablet.serialize(outputStream);
    // Write isAligned at the end
    final boolean isAligned = true;
    ReadWriteIOUtils.write(isAligned, outputStream);

    final ByteBuffer buffer =
        ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());

    // Deserialize Statement from Tablet format
    final InsertTabletStatement statement =
        TabletStatementConverter.deserializeStatementFromTabletFormat(buffer);

    // Verify basic information
    Assert.assertEquals(deviceName, statement.getDevicePath().getFullPath());
    Assert.assertEquals(rowCount, statement.getRowCount());
    Assert.assertEquals(columnCount, statement.getMeasurements().length);
    Assert.assertEquals(isAligned, statement.isAligned());

    // Verify data by converting Statement back to Tablet
    final Tablet convertedTablet = statement.convertToTablet();
    assertTabletsEqual(originalTablet, convertedTablet);
  }

  @Test
  public void testRoundTripConversionTreeModel() throws MetadataException, IOException {
    final int columnCount = 1000;
    final int rowCount = 100;
    final String deviceName = "root.sg.device";

    // Generate original Tablet
    final Tablet originalTablet = generateTreeModelTablet(deviceName, columnCount, rowCount);

    // Serialize Tablet to ByteBuffer
    final PublicBAOS byteArrayOutputStream = new PublicBAOS();
    final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
    originalTablet.serialize(outputStream);
    final boolean isAligned = true;
    ReadWriteIOUtils.write(isAligned, outputStream);
    final ByteBuffer buffer =
        ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());

    // Deserialize to Statement
    final InsertTabletStatement statement =
        TabletStatementConverter.deserializeStatementFromTabletFormat(buffer);
    // Convert Statement back to Tablet
    final Tablet convertedTablet = statement.convertToTablet();

    // Verify round trip
    assertTabletsEqual(originalTablet, convertedTablet);
  }

  @Test
  public void testRoundTripConversionTableModel() throws MetadataException {
    final int columnCount = 1000;
    final int rowCount = 100;
    final String tableName = "table1";
    final String databaseName = "test_db";
    final boolean isAligned = false;

    // Generate original Tablet for table model
    final Tablet originalTablet = generateTableModelTablet(tableName, columnCount, rowCount);

    // Construct Statement from Tablet
    final InsertTabletStatement originalStatement =
        new InsertTabletStatement(originalTablet, isAligned, databaseName);

    // Convert Statement to Tablet
    final Tablet convertedTablet = originalStatement.convertToTablet();

    // Convert Tablet back to Statement
    final InsertTabletStatement convertedStatement =
        new InsertTabletStatement(convertedTablet, isAligned, databaseName);

    // Verify round trip: original Tablet should equal converted Tablet
    assertTabletsEqual(originalTablet, convertedTablet);
  }

  /**
   * Generate a Tablet for tree model with all data types and specified number of columns and rows.
   *
   * @param deviceName device name
   * @param columnCount number of columns
   * @param rowCount number of rows
   * @return Tablet with test data
   */
  private Tablet generateTreeModelTablet(
      final String deviceName, final int columnCount, final int rowCount) {
    final List<IMeasurementSchema> schemaList = new ArrayList<>();
    final TSDataType[] dataTypes = new TSDataType[columnCount];
    final String[] measurementNames = new String[columnCount];
    final Object[] columnData = new Object[columnCount];

    // Create schemas and generate test data
    for (int col = 0; col < columnCount; col++) {
      final TSDataType dataType = ALL_DATA_TYPES[col % ALL_DATA_TYPES.length];
      final String measurementName = "col_" + col + "_" + dataType.name();
      schemaList.add(new MeasurementSchema(measurementName, dataType));
      dataTypes[col] = dataType;
      measurementNames[col] = measurementName;

      // Generate test data for this column
      switch (dataType) {
        case BOOLEAN:
          final boolean[] boolValues = new boolean[rowCount];
          for (int row = 0; row < rowCount; row++) {
            boolValues[row] = (row + col) % 2 == 0;
          }
          columnData[col] = boolValues;
          break;
        case INT32:
          final int[] intValues = new int[rowCount];
          for (int row = 0; row < rowCount; row++) {
            intValues[row] = row * 100 + col;
          }
          columnData[col] = intValues;
          break;
        case DATE:
          final LocalDate[] dateValues = new LocalDate[rowCount];
          for (int row = 0; row < rowCount; row++) {
            // Generate valid dates starting from 2024-01-01
            dateValues[row] = LocalDate.of(2024, 1, 1).plusDays((row + col) % 365);
          }
          columnData[col] = dateValues;
          break;
        case INT64:
        case TIMESTAMP:
          final long[] longValues = new long[rowCount];
          for (int row = 0; row < rowCount; row++) {
            longValues[row] = (long) row * 1000L + col;
          }
          columnData[col] = longValues;
          break;
        case FLOAT:
          final float[] floatValues = new float[rowCount];
          for (int row = 0; row < rowCount; row++) {
            floatValues[row] = row * 1.5f + col * 0.1f;
          }
          columnData[col] = floatValues;
          break;
        case DOUBLE:
          final double[] doubleValues = new double[rowCount];
          for (int row = 0; row < rowCount; row++) {
            doubleValues[row] = row * 2.5 + col * 0.01;
          }
          columnData[col] = doubleValues;
          break;
        case TEXT:
        case STRING:
        case BLOB:
          final Binary[] binaryValues = new Binary[rowCount];
          for (int row = 0; row < rowCount; row++) {
            binaryValues[row] = new Binary(("value_row_" + row + "_col_" + col).getBytes());
          }
          columnData[col] = binaryValues;
          break;
        default:
          throw new IllegalArgumentException("Unsupported data type: " + dataType);
      }
    }

    // Create and fill tablet
    final Tablet tablet = new Tablet(deviceName, schemaList, rowCount);
    final long[] times = new long[rowCount];
    for (int row = 0; row < rowCount; row++) {
      times[row] = row * 1000L;
      final int rowIndex = tablet.getRowSize();
      tablet.addTimestamp(rowIndex, times[row]);
      for (int col = 0; col < columnCount; col++) {
        final TSDataType dataType = dataTypes[col];
        final Object data = columnData[col];
        switch (dataType) {
          case BOOLEAN:
            tablet.addValue(measurementNames[col], rowIndex, ((boolean[]) data)[row]);
            break;
          case INT32:
            tablet.addValue(measurementNames[col], rowIndex, ((int[]) data)[row]);
            break;
          case DATE:
            tablet.addValue(measurementNames[col], rowIndex, ((LocalDate[]) data)[row]);
            break;
          case INT64:
          case TIMESTAMP:
            tablet.addValue(measurementNames[col], rowIndex, ((long[]) data)[row]);
            break;
          case FLOAT:
            tablet.addValue(measurementNames[col], rowIndex, ((float[]) data)[row]);
            break;
          case DOUBLE:
            tablet.addValue(measurementNames[col], rowIndex, ((double[]) data)[row]);
            break;
          case TEXT:
          case STRING:
          case BLOB:
            tablet.addValue(measurementNames[col], rowIndex, ((Binary[]) data)[row]);
            break;
        }
      }
    }

    return tablet;
  }

  /**
   * Generate a Tablet for table model with all data types and specified number of columns and rows.
   *
   * @param tableName table name
   * @param columnCount number of columns
   * @param rowCount number of rows
   * @return Tablet with test data
   */
  private Tablet generateTableModelTablet(
      final String tableName, final int columnCount, final int rowCount) {
    final List<IMeasurementSchema> schemaList = new ArrayList<>();
    final TSDataType[] dataTypes = new TSDataType[columnCount];
    final String[] measurementNames = new String[columnCount];
    final List<ColumnCategory> columnTypes = new ArrayList<>();
    final Object[] columnData = new Object[columnCount];

    // Create schemas and generate test data
    for (int col = 0; col < columnCount; col++) {
      final TSDataType dataType = ALL_DATA_TYPES[col % ALL_DATA_TYPES.length];
      final String measurementName = "col_" + col + "_" + dataType.name();
      schemaList.add(new MeasurementSchema(measurementName, dataType));
      dataTypes[col] = dataType;
      measurementNames[col] = measurementName;
      // For table model, all columns are FIELD (can be TAG/ATTRIBUTE/FIELD, but we use FIELD for
      // simplicity)
      columnTypes.add(ColumnCategory.FIELD);

      // Generate test data for this column
      switch (dataType) {
        case BOOLEAN:
          final boolean[] boolValues = new boolean[rowCount];
          for (int row = 0; row < rowCount; row++) {
            boolValues[row] = (row + col) % 2 == 0;
          }
          columnData[col] = boolValues;
          break;
        case INT32:
          final int[] intValues = new int[rowCount];
          for (int row = 0; row < rowCount; row++) {
            intValues[row] = row * 100 + col;
          }
          columnData[col] = intValues;
          break;
        case DATE:
          final LocalDate[] dateValues = new LocalDate[rowCount];
          for (int row = 0; row < rowCount; row++) {
            // Generate valid dates starting from 2024-01-01
            dateValues[row] = LocalDate.of(2024, 1, 1).plusDays((row + col) % 365);
          }
          columnData[col] = dateValues;
          break;
        case INT64:
        case TIMESTAMP:
          final long[] longValues = new long[rowCount];
          for (int row = 0; row < rowCount; row++) {
            longValues[row] = (long) row * 1000L + col;
          }
          columnData[col] = longValues;
          break;
        case FLOAT:
          final float[] floatValues = new float[rowCount];
          for (int row = 0; row < rowCount; row++) {
            floatValues[row] = row * 1.5f + col * 0.1f;
          }
          columnData[col] = floatValues;
          break;
        case DOUBLE:
          final double[] doubleValues = new double[rowCount];
          for (int row = 0; row < rowCount; row++) {
            doubleValues[row] = row * 2.5 + col * 0.01;
          }
          columnData[col] = doubleValues;
          break;
        case TEXT:
        case STRING:
        case BLOB:
          final Binary[] binaryValues = new Binary[rowCount];
          for (int row = 0; row < rowCount; row++) {
            binaryValues[row] = new Binary(("value_row_" + row + "_col_" + col).getBytes());
          }
          columnData[col] = binaryValues;
          break;
        default:
          throw new IllegalArgumentException("Unsupported data type: " + dataType);
      }
    }

    // Create tablet using table model constructor: Tablet(String, List<String>, List<TSDataType>,
    // List<ColumnCategory>, int)
    final List<String> measurementNameList = IMeasurementSchema.getMeasurementNameList(schemaList);
    final List<TSDataType> dataTypeList = IMeasurementSchema.getDataTypeList(schemaList);
    final Tablet tablet =
        new Tablet(tableName, measurementNameList, dataTypeList, columnTypes, rowCount);
    tablet.initBitMaps();

    // Fill tablet with data
    final long[] times = new long[rowCount];
    for (int row = 0; row < rowCount; row++) {
      times[row] = row * 1000L;
      final int rowIndex = tablet.getRowSize();
      tablet.addTimestamp(rowIndex, times[row]);
      for (int col = 0; col < columnCount; col++) {
        final TSDataType dataType = dataTypes[col];
        final Object data = columnData[col];
        switch (dataType) {
          case BOOLEAN:
            tablet.addValue(measurementNames[col], rowIndex, ((boolean[]) data)[row]);
            break;
          case INT32:
            tablet.addValue(measurementNames[col], rowIndex, ((int[]) data)[row]);
            break;
          case DATE:
            tablet.addValue(measurementNames[col], rowIndex, ((LocalDate[]) data)[row]);
            break;
          case INT64:
          case TIMESTAMP:
            tablet.addValue(measurementNames[col], rowIndex, ((long[]) data)[row]);
            break;
          case FLOAT:
            tablet.addValue(measurementNames[col], rowIndex, ((float[]) data)[row]);
            break;
          case DOUBLE:
            tablet.addValue(measurementNames[col], rowIndex, ((double[]) data)[row]);
            break;
          case TEXT:
          case STRING:
          case BLOB:
            tablet.addValue(measurementNames[col], rowIndex, ((Binary[]) data)[row]);
            break;
        }
      }
      tablet.setRowSize(rowIndex + 1);
    }

    return tablet;
  }

  /**
   * Check if two Tablets are equal in all aspects.
   *
   * @param expected expected Tablet
   * @param actual actual Tablet
   */
  private void assertTabletsEqual(final Tablet expected, final Tablet actual) {
    Assert.assertEquals(expected.getDeviceId(), actual.getDeviceId());
    Assert.assertEquals(expected.getRowSize(), actual.getRowSize());
    Assert.assertEquals(expected.getSchemas().size(), actual.getSchemas().size());

    // Verify timestamps
    final long[] expectedTimes = expected.getTimestamps();
    final long[] actualTimes = actual.getTimestamps();
    Assert.assertArrayEquals(expectedTimes, actualTimes);

    // Verify each column
    final int columnCount = expected.getSchemas().size();
    final int rowCount = expected.getRowSize();
    final Object[] expectedValues = expected.getValues();
    final Object[] actualValues = actual.getValues();

    for (int col = 0; col < columnCount; col++) {
      final IMeasurementSchema schema = expected.getSchemas().get(col);
      final TSDataType dataType = schema.getType();
      final Object expectedColumn = expectedValues[col];
      final Object actualColumn = actualValues[col];

      Assert.assertNotNull(actualColumn);

      // Verify each row in this column
      for (int row = 0; row < rowCount; row++) {
        switch (dataType) {
          case BOOLEAN:
            final boolean expectedBool = ((boolean[]) expectedColumn)[row];
            final boolean actualBool = ((boolean[]) actualColumn)[row];
            Assert.assertEquals(expectedBool, actualBool);
            break;
          case INT32:
            final int expectedInt = ((int[]) expectedColumn)[row];
            final int actualInt = ((int[]) actualColumn)[row];
            Assert.assertEquals(expectedInt, actualInt);
            break;
          case DATE:
            final LocalDate expectedDate = ((LocalDate[]) expectedColumn)[row];
            final LocalDate actualDate = ((LocalDate[]) actualColumn)[row];
            Assert.assertEquals(expectedDate, actualDate);
            break;
          case INT64:
          case TIMESTAMP:
            final long expectedLong = ((long[]) expectedColumn)[row];
            final long actualLong = ((long[]) actualColumn)[row];
            Assert.assertEquals(expectedLong, actualLong);
            break;
          case FLOAT:
            final float expectedFloat = ((float[]) expectedColumn)[row];
            final float actualFloat = ((float[]) actualColumn)[row];
            Assert.assertEquals(expectedFloat, actualFloat, 0.0001f);
            break;
          case DOUBLE:
            final double expectedDouble = ((double[]) expectedColumn)[row];
            final double actualDouble = ((double[]) actualColumn)[row];
            Assert.assertEquals(expectedDouble, actualDouble, 0.0001);
            break;
          case TEXT:
          case STRING:
          case BLOB:
            final Binary expectedBinary = ((Binary[]) expectedColumn)[row];
            final Binary actualBinary = ((Binary[]) actualColumn)[row];
            Assert.assertNotNull(actualBinary);
            Assert.assertEquals(expectedBinary, actualBinary);
            break;
        }
      }
    }
  }

  /**
   * Check if a Tablet and an InsertTabletStatement contain the same data.
   *
   * @param tablet Tablet
   * @param statement InsertTabletStatement
   */
  private void assertTabletAndStatementEqual(
      final Tablet tablet, final InsertTabletStatement statement) {
    Assert.assertEquals(
        tablet.getDeviceId(),
        statement.getDevicePath() != null ? statement.getDevicePath().getFullPath() : null);
    Assert.assertEquals(tablet.getRowSize(), statement.getRowCount());
    Assert.assertEquals(tablet.getSchemas().size(), statement.getMeasurements().length);

    // Verify timestamps
    Assert.assertArrayEquals(tablet.getTimestamps(), statement.getTimes());

    // Verify each column
    final int columnCount = tablet.getSchemas().size();
    final int rowCount = tablet.getRowSize();
    final Object[] tabletValues = tablet.getValues();
    final Object[] statementColumns = statement.getColumns();

    for (int col = 0; col < columnCount; col++) {
      final TSDataType dataType = tablet.getSchemas().get(col).getType();
      final Object tabletColumn = tabletValues[col];
      final Object statementColumn = statementColumns[col];

      Assert.assertNotNull(statementColumn);

      // Verify each row
      for (int row = 0; row < rowCount; row++) {
        switch (dataType) {
          case BOOLEAN:
            final boolean tabletBool = ((boolean[]) tabletColumn)[row];
            final boolean statementBool = ((boolean[]) statementColumn)[row];
            Assert.assertEquals(tabletBool, statementBool);
            break;
          case INT32:
            final int tabletInt = ((int[]) tabletColumn)[row];
            final int statementInt = ((int[]) statementColumn)[row];
            Assert.assertEquals(tabletInt, statementInt);
            break;
          case DATE:
            // DATE type: Tablet uses LocalDate[], Statement uses int[] (YYYYMMDD format)
            final LocalDate tabletDate = ((LocalDate[]) tabletColumn)[row];
            final int statementDateInt = ((int[]) statementColumn)[row];
            // Convert LocalDate to int (YYYYMMDD format) for comparison
            final int tabletDateInt = DateUtils.parseDateExpressionToInt(tabletDate);
            Assert.assertEquals(tabletDateInt, statementDateInt);
            break;
          case INT64:
          case TIMESTAMP:
            final long tabletLong = ((long[]) tabletColumn)[row];
            final long statementLong = ((long[]) statementColumn)[row];
            Assert.assertEquals(tabletLong, statementLong);
            break;
          case FLOAT:
            final float tabletFloat = ((float[]) tabletColumn)[row];
            final float statementFloat = ((float[]) statementColumn)[row];
            Assert.assertEquals(tabletFloat, statementFloat, 0.0001f);
            break;
          case DOUBLE:
            final double tabletDouble = ((double[]) tabletColumn)[row];
            final double statementDouble = ((double[]) statementColumn)[row];
            Assert.assertEquals(tabletDouble, statementDouble, 0.0001);
            break;
          case TEXT:
          case STRING:
          case BLOB:
            final Binary tabletBinary = ((Binary[]) tabletColumn)[row];
            final Binary statementBinary = ((Binary[]) statementColumn)[row];
            Assert.assertNotNull(statementBinary);
            Assert.assertEquals(tabletBinary, statementBinary);
            break;
        }
      }
    }
  }

  // Define all supported data types
  private static final TSDataType[] ALL_DATA_TYPES = {
    TSDataType.BOOLEAN,
    TSDataType.INT32,
    TSDataType.INT64,
    TSDataType.FLOAT,
    TSDataType.DOUBLE,
    TSDataType.TEXT,
    TSDataType.TIMESTAMP,
    TSDataType.DATE,
    TSDataType.BLOB,
    TSDataType.STRING
  };
}
