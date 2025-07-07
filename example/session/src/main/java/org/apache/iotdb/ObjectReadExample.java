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

package org.apache.iotdb;

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.TableSessionBuilder;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.tsfile.utils.Binary;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;

public class ObjectReadExample {
  private static final String LOCAL_URL = "127.0.0.1:6667";

  public static void main(String[] args) {

    // don't specify database in constructor
    try (ITableSession session =
        new TableSessionBuilder()
            .nodeUrls(Collections.singletonList(LOCAL_URL))
            .username("root")
            .password("root")
            .database("test1")
            .thriftMaxFrameSize(256 * 1024 * 1024)
            .build()) {
      try (SessionDataSet dataSet =
          session.executeQueryStatement("select READ_OBJECT(file) from test1 where time = 1")) {
        SessionDataSet.DataIterator iterator = dataSet.iterator();
        while (iterator.next()) {
          Binary binary = iterator.getBlob(1);
          System.out.println(DigestUtils.md5Hex(binary.getValues()));
        }
      }

      try (SessionDataSet dataSet =
          session.executeQueryStatement("select READ_OBJECT(file) from test1 where time = 2")) {
        SessionDataSet.DataIterator iterator = dataSet.iterator();
        while (iterator.next()) {
          Binary binary = iterator.getBlob(1);
          System.out.println(DigestUtils.md5Hex(binary.getValues()));
        }
      }

      try (SessionDataSet dataSet =
          session.executeQueryStatement("select READ_OBJECT(file) from test1")) {
        SessionDataSet.DataIterator iterator = dataSet.iterator();
        while (iterator.next()) {
          Binary binary = iterator.getBlob(1);
          System.out.println(DigestUtils.md5Hex(binary.getValues()));
        }
      }

      try (SessionDataSet dataSet =
          session.executeQueryStatement("select geo_penetrate(file, '0,3,7501,7504') from test1")) {
        SessionDataSet.DataIterator iterator = dataSet.iterator();
        while (iterator.next()) {
          Binary binary = iterator.getBlob(1);
          ByteBuffer byteBuffer = ByteBuffer.wrap(binary.getValues());
          float[] res = new float[byteBuffer.limit() / Float.BYTES];
          for (int i = 0; i < res.length; i++) {
            res[i] = byteBuffer.getFloat();
          }
          System.out.println(Arrays.toString(res));
        }
      }

    } catch (IoTDBConnectionException e) {
      e.printStackTrace();
    } catch (StatementExecutionException e) {
      e.printStackTrace();
    }
  }
}
