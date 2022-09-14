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

import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.util.Version;

public class Testt {
    public static void main(String[] args) throws Exception{
        query();
    }

    private static void query() throws Exception{
        Session session =
                new Session.Builder()
                        .host("127.0.0.1")
                        .port(6667)
                        .username("root")
                        .password("root")
                        .version(Version.V_0_13)
                        .build();
        session.open(false);
        // set session fetchSize
        session.setFetchSize(10000);
        long time1 = System.currentTimeMillis();
        session.executeQueryStatement("select s1 from root.**;");
        long time2 = System.currentTimeMillis();
        System.out.println(time2-time1);
        session.close();
    }

    private static void insert() throws Exception{
        Session session =
                new Session.Builder()
                        .host("127.0.0.1")
                        .port(6667)
                        .username("root")
                        .password("root")
                        .version(Version.V_0_13)
                        .build();
        session.open(false);
        // set session fetchSize
        session.setFetchSize(10000);
//        for (int i = 0; i < 500; i++) {
//            for (int j = 0; j < 400; j++) {
//                session.executeNonQueryStatement(String.format("insert into root.sg1.d%d(timestamp,s%d) values(1,1.0);",i,j));
//                session.executeNonQueryStatement(String.format("insert into root.sg1.d%d(timestamp,s%d) values(2,2.0);",i,j));
//                session.executeNonQueryStatement(String.format("insert into root.sg1.d%d(timestamp,s%d) values(3,3.0);",i,j));
//            }
//        }
//        session.executeNonQueryStatement("flush;");
//        for (int i = 0; i < 500; i++) {
//            for (int j = 0; j < 400; j++) {
//                session.executeNonQueryStatement(String.format("insert into root.sg1.d%d(timestamp,s%d) values(4,4.0);",i,j));
//                session.executeNonQueryStatement(String.format("insert into root.sg1.d%d(timestamp,s%d) values(5,5.0);",i,j));
//                session.executeNonQueryStatement(String.format("insert into root.sg1.d%d(timestamp,s%d) values(6,6.0);",i,j));
//            }
//        }
//        session.executeNonQueryStatement("flush;");
        for (int j = 0; j < 400; j++) {
            session.executeNonQueryStatement(String.format("delete from root.*.*.s%d where time>2 and time<5;",j));
        }
        session.close();
    }
}
