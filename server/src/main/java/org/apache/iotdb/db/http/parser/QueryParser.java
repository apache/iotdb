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
package org.apache.iotdb.db.http.parser;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.runtime.SQLParserException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.constant.DatetimeUtils;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.logical.crud.BasicFunctionOperator;
import org.apache.iotdb.db.qp.logical.crud.FilterOperator;
import org.apache.iotdb.db.qp.logical.crud.FromOperator;
import org.apache.iotdb.db.qp.logical.crud.QueryOperator;
import org.apache.iotdb.db.qp.logical.crud.SelectOperator;
import org.apache.iotdb.tsfile.utils.Pair;

public class QueryParser {
  /**
   * generate select statement operator
   */
  public static QueryOperator generateOperator(String suffixPath, String prefixPath,
      Pair<String, String> timeRange) throws IllegalPathException {
    FilterOperator binaryOp = new FilterOperator(SQLConstant.KW_AND);
    long timeLeft;
    long timeRight;
    if(!NumberUtils.isDigits(timeRange.left)) {
      timeLeft = parseTimeFormat(timeRange.left);
      binaryOp.addChildOperator(
          new BasicFunctionOperator(SQLConstant.GREATERTHAN,
              SQLConstant.TIME_PATH,
              String.valueOf(timeLeft)
          )
      );
    } else {
      binaryOp.addChildOperator(
          new BasicFunctionOperator(SQLConstant.GREATERTHAN,
              SQLConstant.TIME_PATH,
              timeRange.left
          )
      );
    }

    if(!NumberUtils.isDigits(timeRange.right)) {
      timeRight = parseTimeFormat(timeRange.right);
      binaryOp.addChildOperator(
          new BasicFunctionOperator(SQLConstant.LESSTHAN,
              SQLConstant.TIME_PATH,
              String.valueOf(timeRight)
          )
      );
    } else {
      binaryOp.addChildOperator(
          new BasicFunctionOperator(SQLConstant.LESSTHAN,
              SQLConstant.TIME_PATH,
              timeRange.right
          )
      );
    }
    QueryOperator queryOp = new QueryOperator(SQLConstant.TOK_QUERY);
    SelectOperator selectOp = new SelectOperator(SQLConstant.TOK_SELECT);
    selectOp.addSelectPath(new PartialPath(suffixPath));
    FromOperator fromOp = new FromOperator(SQLConstant.TOK_FROM);
    fromOp.addPrefixTablePath(new PartialPath(prefixPath));
    queryOp.setFilterOperator(binaryOp);
    queryOp.setSelectOperator(selectOp);
    queryOp.setFromOperator(fromOp);
    return queryOp;
  }

  /**
   * function for parsing time format.
   */
  private static long parseTimeFormat(String timestampStr) {
    if (timestampStr == null || timestampStr.trim().equals("")) {
      throw new SQLParserException("input timestamp cannot be empty");
    }
    if (timestampStr.equalsIgnoreCase(SQLConstant.NOW_FUNC)) {
      return System.currentTimeMillis();
    }
    try {
      return DatetimeUtils
          .convertDatetimeStrToLong(timestampStr, IoTDBDescriptor.getInstance().getConfig().getZoneID());
    } catch (Exception e) {
      throw new SQLParserException(String
          .format("Input time format %s error. "
              + "Input like yyyy-MM-dd HH:mm:ss, yyyy-MM-ddTHH:mm:ss or "
              + "refer to user document for more info.", timestampStr));
    }
  }
}
