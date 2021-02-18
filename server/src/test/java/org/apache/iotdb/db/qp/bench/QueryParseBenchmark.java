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
package org.apache.iotdb.db.qp.bench;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;

/**
 * SQL Parse benchmark. This class is used to get the performance of SQL Parse. It contains：Insert
 * SQL
 */
public class QueryParseBenchmark {

  private static int numOfBenchSQL = 100000;
  private static String insertSQL =
      "insert into root.perform.group_3.d_6(timestamp,s_0,s_1,s_2,s_3,s_4,s_5,s_6,s_7,s_8,s_9,s_10,s_11,s_12,s_13,s_14,s_15,s_16,s_17,s_18,s_19,s_20,s_21,s_22,s_23,s_24,s_25,s_26,s_27,s_28,s_29,s_30,s_31,s_32,s_33,s_34,s_35,s_36,s_37,s_38,s_39,s_40,s_41,s_42,s_43,s_44,s_45,s_46,s_47,s_48,s_49,s_50,s_51,s_52,s_53,s_54,s_55,s_56,s_57,s_58,s_59,s_60,s_61,s_62,s_63,s_64,s_65,s_66,s_67,s_68,s_69,s_70,s_71,s_72,s_73,s_74,s_75,s_76,s_77,s_78,s_79,s_80,s_81,s_82,s_83,s_84,s_85,s_86,s_87,s_88,s_89,s_90,s_91,s_92,s_93,s_94,s_95,s_96,s_97,s_98,s_99) values(1535558845000,6.651756751280603,6.651756751280603,787.74,0.0,0.0,787.74,6.651756751280603,0.0,33960.24564285714,33960.24564285714,787.74,787.74,33960.24564285714,6.651756751280603,372.5645996161876,19.185434416303107,787.74,728.2850866795781,33960.24564285714,6.651756751280603,33960.24564285714,38.51408765504662,1160.4112366716558,0.0,33960.24564285714,33960.24564285714,6.651756751280603,33960.24564285714,787.74,6.651756751280603,0.0,6.651756751280603,33960.24564285714,787.74,787.74,656.2029144236096,593.309858647129,787.74,0.0,787.74,0.0,33960.24564285714,33960.24564285714,1127.350328754438,33960.24564285714,33960.24564285714,33960.24564285714,787.74,1180.971235067114,661.7070251519021,33960.24564285714,0.0,0.0,1205.10868101537,792.940812264317,0.0,573.1607619219858,6.651756751280603,156.47709039629825,33960.24564285714,809.2345221425896,0.0,6.651756751280603,787.74,6.651756751280603,33960.24564285714,33960.24564285714,965.7042945108797,787.74,6.651756751280603,6.651756751280603,787.74,6.651756751280603,0.0,6.651756751280603,0.0,6.651756751280603,317.6444399747479,787.74,0.0,787.74,6.651756751280603,33960.24564285714,1028.5886481610971,107.99305540655205,0.0,33960.24564285714,787.74,252.59133785592041,725.8255243198603,443.50233343042555,787.74,6.651756751280603,6.651756751280603,33960.24564285714,33960.24564285714,33960.24564285714,787.74,33960.24564285714,441.68146442890685)";

  public static void main(String[] args) throws QueryProcessException {
    Planner planner = new Planner();
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < numOfBenchSQL; i++) {
      PhysicalPlan physicalPlan = planner.parseSQLToPhysicalPlan(insertSQL);
      physicalPlan.isQuery();
    }
    long endTime = System.currentTimeMillis();
    System.out.println(String.format("The total time: %d ms", (endTime - startTime)));
  }
}
