/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.jdbc;

import static org.junit.Assert.*;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class IoTDBResultMetadataTest {
    private IoTDBResultMetadata metadata;

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testGetColumnCount() throws SQLException {
        metadata = new IoTDBResultMetadata(null, null, null);
        boolean flag = false;
        try {
            metadata.getColumnCount();
        } catch (Exception e) {
            flag = true;
        }
        assertEquals(flag, true);

        List<String> columnInfoList = new ArrayList<>();
        flag = false;
        try {
            metadata = new IoTDBResultMetadata(columnInfoList, null, null);
            metadata.getColumnCount();
        } catch (Exception e) {
            flag = true;
        }
        assertEquals(flag, true);

        columnInfoList.add("root.a.b.c");
        assertEquals(metadata.getColumnCount(), 1);
    }

    @Test
    public void testGetColumnName() throws SQLException {
        metadata = new IoTDBResultMetadata(null, null, null);
        boolean flag = false;
        try {
            metadata.getColumnName(1);
        } catch (Exception e) {
            flag = true;
        }
        assertEquals(flag, true);

        List<String> columnInfoList = new ArrayList<>();
        metadata = new IoTDBResultMetadata(columnInfoList, null, null);
        flag = false;
        try {
            metadata.getColumnName(1);
        } catch (Exception e) {
            flag = true;
        }
        assertEquals(flag, true);

        String[] colums = { "root.a.b.c1", "root.a.b.c2", "root.a.b.c3" };
        metadata = new IoTDBResultMetadata(Arrays.asList(colums), null, null);
        flag = false;
        try {
            metadata.getColumnName(colums.length + 1);
        } catch (Exception e) {
            flag = true;
        }
        assertEquals(flag, true);

        flag = false;
        try {
            metadata.getColumnName(0);
        } catch (Exception e) {
            flag = true;
        }
        assertEquals(flag, true);

        for (int i = 1; i <= colums.length; i++) {
            assertEquals(metadata.getColumnLabel(i), colums[i - 1]);
        }

    }

    @Test
    public void testGetColumnType() throws SQLException {
        metadata = new IoTDBResultMetadata(null, null, null);
        boolean flag = false;
        try {
            metadata.getColumnType(1);
        } catch (Exception e) {
            flag = true;
        }
        assertEquals(flag, true);

        List<String> columnInfoList = new ArrayList<>();
        metadata = new IoTDBResultMetadata(columnInfoList, null, null);
        flag = false;
        try {
            metadata.getColumnType(1);
        } catch (Exception e) {
            flag = true;
        }
        assertEquals(flag, true);

        String[] columns = { "timestamp", "root.a.b.boolean", "root.a.b.int32", "root.a.b.int64", "root.a.b.float",
                "root.a.b.double", "root.a.b.text" };
        String[] typesString = { "BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT" };
        int[] types = { Types.BOOLEAN, Types.INTEGER, Types.BIGINT, Types.FLOAT, Types.DOUBLE, Types.VARCHAR };
        metadata = new IoTDBResultMetadata(Arrays.asList(columns), null, Arrays.asList(typesString));
        flag = false;
        try {
            metadata.getColumnType(columns.length + 1);
        } catch (Exception e) {
            flag = true;
        }
        assertEquals(flag, true);

        flag = false;
        try {
            metadata.getColumnType(0);
        } catch (Exception e) {
            flag = true;
        }
        assertEquals(flag, true);

        assertEquals(metadata.getColumnType(1), Types.TIMESTAMP);
        for (int i = 1; i <= types.length; i++) {
            assertEquals(metadata.getColumnType(i + 1), types[i - 1]);
        }
    }

    @Test
    public void testGetColumnTypeName() throws SQLException {
        String operationType = "sum";
        metadata = new IoTDBResultMetadata(null, operationType, null);
        assertEquals(metadata.getColumnTypeName(1), operationType);
    }

}
