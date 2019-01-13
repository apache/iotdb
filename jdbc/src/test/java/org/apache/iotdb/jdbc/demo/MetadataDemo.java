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
package org.apache.iotdb.jdbc.demo;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import org.apache.iotdb.jdbc.Config;

public class MetadataDemo {

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        // Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        // Connection connection = null;
        // try {
        // connection = DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        // DatabaseMetaData databaseMetaData = connection.getMetaData();
        // } finally {
        // connection.close();
        // }
    }

}