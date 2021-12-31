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
package org.apache.iotdb.library.anomaly;

import org.apache.iotdb.library.util.DataGeneration_0_12_0;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.Session;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.jsonwebtoken.lang.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;

public class AnomalyTest {
  @BeforeAll
  static void generateData() throws IOException, InterruptedException {
    String systemName = System.getProperty("os.name");
    if (systemName.toLowerCase(Locale.ROOT).contains("windows")) {
      DataGeneration_0_12_0.generateData("cmd");
    } else {
      DataGeneration_0_12_0.generateData("bash");
    }
  }

  @Test
  public void test() throws IoTDBConnectionException, IOException, InterruptedException {
    final String[] udfList = {
      "UDTFIQR", "UDTFKSigma", "UDTFLOF", "UDTFMissDetect", "UDTFRange", "UDTFTwoSidedFilter"
    };
    Gson gson = new Gson();
    Reader reader = Files.newBufferedReader(Paths.get("src/test/resources/AnomalyQueries.json"));
    HashMap<String, ArrayList<String>> udfQueries =
        gson.fromJson(reader, new TypeToken<HashMap<String, ArrayList<String>>>() {}.getType());
    reader.close();

    Assert.isTrue(udfList.length == udfQueries.size());
    Session session;
    try {
      session = new Session("127.0.0.1", "6667", "root", "root");
      session.open();
    } catch (Exception e) {
      System.out.println(
          "Failed to connect to session. Please start IoTDB and use default host, port, username and password.");
      System.out.println(e.getMessage());
      return;
    }
    for (String functionName : udfList) {
      try { // register function
        session.executeNonQueryStatement("drop function " + functionName);
        session.executeNonQueryStatement(
            "create function "
                + functionName
                + " as 'org.apache.iotdb.library.anomaly."
                + functionName
                + "'");
      } catch (Exception e) {
        try {
          session.executeNonQueryStatement(
              "create function "
                  + functionName
                  + " as 'org.apache.iotdb.library.anomaly."
                  + functionName
                  + "'");
        } catch (Exception f) {
          System.out.println("Cannot register function " + functionName + ".");
          System.out.println(f.getMessage());
          continue;
        }
      }

      // run query
      ArrayList<String> queries = udfQueries.get(functionName);
      for (String query : queries) {
        try {
          session.executeQueryStatement(query);
        } catch (Exception e) {
          System.out.println("Query failed at function " + functionName);
          System.out.println(e.getMessage());
        }
        Thread.sleep(1000);
      }

      try { // drop function
        session.executeNonQueryStatement("drop function " + functionName);
      } catch (Exception ignored) {
      }
    }
    session.close();
  }
}
