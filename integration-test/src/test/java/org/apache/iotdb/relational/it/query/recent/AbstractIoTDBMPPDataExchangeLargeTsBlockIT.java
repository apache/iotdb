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

package org.apache.iotdb.relational.it.query.recent;

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.it.env.EnvFactory;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public abstract class AbstractIoTDBMPPDataExchangeLargeTsBlockIT {

  protected static final String DATABASE_NAME = "large_tsblock";
  protected static final int PAYLOAD_SIZE_IN_BYTES = 128 * 1024;
  private static final int BLOB_SIZE_IN_BYTES = 2 * PAYLOAD_SIZE_IN_BYTES + 1;
  private static final byte[] EXPECTED_BLOB = createBlob();

  protected static void prepareData() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + DATABASE_NAME);
      statement.execute("USE " + DATABASE_NAME);
      statement.execute("CREATE TABLE large_blob(payload BLOB FIELD)");
    }

    List<IMeasurementSchema> schemas = List.of(new MeasurementSchema("payload", TSDataType.BLOB));
    Tablet tablet =
        new Tablet(
            "large_blob",
            IMeasurementSchema.getMeasurementNameList(schemas),
            IMeasurementSchema.getDataTypeList(schemas),
            List.of(ColumnCategory.FIELD),
            1);
    tablet.addTimestamp(0, 1);
    tablet.addValue("payload", 0, new Binary(EXPECTED_BLOB));

    try (ITableSession session =
        EnvFactory.getEnv().getTableSessionConnectionWithDB(DATABASE_NAME)) {
      session.insert(tablet);
    }
  }

  @Test
  public void testLargeBlobTransferredInFragments() throws Exception {
    assertTrue(EXPECTED_BLOB.length > PAYLOAD_SIZE_IN_BYTES);

    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      try (ResultSet resultSet = statement.executeQuery("SELECT time, payload FROM large_blob")) {
        assertTrue(resultSet.next());
        assertEquals(1, resultSet.getLong("time"));
        assertArrayEquals(EXPECTED_BLOB, resultSet.getBytes("payload"));
        assertFalse(resultSet.next());
      }
    }
  }

  private static byte[] createBlob() {
    byte[] blob = new byte[BLOB_SIZE_IN_BYTES];
    for (int i = 0; i < blob.length; i++) {
      blob[i] = (byte) (i * 31 + 7);
    }
    return blob;
  }
}
