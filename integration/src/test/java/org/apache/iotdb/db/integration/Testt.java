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
package org.apache.iotdb.db.integration;

import org.apache.iotdb.jdbc.Config;
import org.junit.Assert;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class Testt {
    public static void main(String[] args) throws Exception{
//        deleteData();
        query();
    }

    public static void deleteData() throws Exception{
        Class.forName(Config.JDBC_DRIVER_NAME);
        FileInputStream fis = new FileInputStream("/Users/chenyanze/Downloads/3247/sql.txt");

        BufferedReader br = new BufferedReader(new InputStreamReader(fis));

        try (Connection connection =
                     DriverManager.getConnection(
                             Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
             Statement statement = connection.createStatement()) {
            String line = null;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
                try {
                    statement.execute(line);
                }catch (Exception e){
                    System.out.println(e.getMessage());
                }
            }
            br.close();

        } catch (Exception e) {
            System.out.println("end");
        }
    }

    public static void query() throws Exception{
        Class.forName(Config.JDBC_DRIVER_NAME);
        FileInputStream fis = new FileInputStream("/Users/chenyanze/Downloads/3247/dev_name.txt");

        BufferedReader br = new BufferedReader(new InputStreamReader(fis));

        try (Connection connection =
                     DriverManager.getConnection(
                             Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
             Statement statement = connection.createStatement()) {
            String line = null;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
                try {
                    statement.execute("select count(s_4) ,count(s_144) from  "+line);
                    ResultSet resultSet = statement.getResultSet();
                    while (resultSet.next()) {
                        System.out.println(resultSet.getString(1)+","+resultSet.getString(2));
                    }
                }catch (Exception e){
                    System.out.println(e.getMessage());
                }
            }
            br.close();

        } catch (Exception e) {
            System.out.println("end");
        }
    }
}
