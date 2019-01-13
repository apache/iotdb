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
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class StatementDemo {

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Class.forName("org.apache.iotdb.jdbc.TsfileDriver");
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            statement.execute("SET STORAGE GROUP TO root.ln.wf01.wt01");
            statement.execute("CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN");
            statement.execute("CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=RLE");
            statement.execute("insert into root.ln.wf01.wt01(timestamp,status) values(1509465600000,true)");
            statement.execute("insert into root.ln.wf01.wt01(timestamp,status) values(1509465660000,true)");
            statement.execute("insert into root.ln.wf01.wt01(timestamp,status) values(1509465720000,false)");
            statement.execute("insert into root.ln.wf01.wt01(timestamp,temperature) values(1509465600000,25.957603)");
            statement.execute("insert into root.ln.wf01.wt01(timestamp,temperature) values(1509465660000,24.359503)");
            statement.execute("insert into root.ln.wf01.wt01(timestamp,temperature) values(1509465720000,20.092794)");
            ResultSet resultSet = statement.executeQuery("select * from root");
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            while (resultSet.next()) {
                StringBuilder builder = new StringBuilder();
                for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                    builder.append(resultSet.getString(i)).append(",");
                }
                System.out.println(builder);
            }
            statement.close();

        } finally {
            connection.close();
        }
    }
}
