/**
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

package org.apache.iotdb.db.query.aggregation;

import org.apache.iotdb.tsfile.common.constant.StatisticConstant;

public class AggregationConstant {

  public static final String MIN_TIME = StatisticConstant.MIN_TIME;
  public static final String MAX_TIME = StatisticConstant.MAX_TIME;

  public static final String MAX_VALUE = StatisticConstant.MAX_VALUE;
  public static final String MIN_VALUE = StatisticConstant.MIN_VALUE;

  public static final String COUNT = StatisticConstant.COUNT;

  public static final String FIRST = StatisticConstant.FIRST;
  public static final String LAST = StatisticConstant.LAST;

  public static final String MEAN = StatisticConstant.MEAN;
  public static final String SUM = StatisticConstant.SUM;

}
