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
package org.apache.iotdb.db.utils;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.constant.SqlConstant;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;

import com.google.common.base.Throwables;
import io.airlift.airline.Cli;
import io.airlift.airline.Help;
import io.airlift.airline.ParseArgumentsMissingException;
import io.airlift.airline.ParseArgumentsUnexpectedException;
import io.airlift.airline.ParseCommandMissingException;
import io.airlift.airline.ParseCommandUnrecognizedException;
import io.airlift.airline.ParseOptionConversionException;
import io.airlift.airline.ParseOptionMissingException;
import io.airlift.airline.ParseOptionMissingValueException;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;

@SuppressWarnings("java:S106") // for console outputs
public class CommonUtils {

  private CommonUtils() {}

  public static Object parseValue(TSDataType dataType, String value) throws QueryProcessException {
    try {
      if ("null".equals(value) || "NULL".equals(value)) {
        return null;
      }
      switch (dataType) {
        case BOOLEAN:
          return parseBoolean(value);
        case INT32:
          try {
            return Integer.parseInt(StringUtils.trim(value));
          } catch (NumberFormatException e) {
            throw new NumberFormatException(
                "data type is not consistent, input " + value + ", registered " + dataType);
          }
        case INT64:
          try {
            return Long.parseLong(StringUtils.trim(value));
          } catch (NumberFormatException e) {
            throw new NumberFormatException(
                "data type is not consistent, input " + value + ", registered " + dataType);
          }
        case FLOAT:
          float f;
          try {
            f = Float.parseFloat(value);
          } catch (NumberFormatException e) {
            throw new NumberFormatException(
                "data type is not consistent, input " + value + ", registered " + dataType);
          }
          if (Float.isInfinite(f)) {
            throw new NumberFormatException("The input float value is Infinity");
          }
          return f;
        case DOUBLE:
          double d;
          try {
            d = Double.parseDouble(value);
          } catch (NumberFormatException e) {
            throw new NumberFormatException(
                "data type is not consistent, input " + value + ", registered " + dataType);
          }
          if (Double.isInfinite(d)) {
            throw new NumberFormatException("The input double value is Infinity");
          }
          return d;
        case TEXT:
          if ((value.startsWith(SqlConstant.QUOTE) && value.endsWith(SqlConstant.QUOTE))
              || (value.startsWith(SqlConstant.DQUOTE) && value.endsWith(SqlConstant.DQUOTE))) {
            if (value.length() == 1) {
              return new Binary(value);
            } else {
              return new Binary(value.substring(1, value.length() - 1));
            }
          }

          return new Binary(value);
        default:
          throw new QueryProcessException("Unsupported data type:" + dataType);
      }
    } catch (NumberFormatException e) {
      throw new QueryProcessException(e.getMessage());
    }
  }

  public static boolean checkCanCastType(TSDataType src, TSDataType dest) {
    switch (src) {
      case INT32:
        if (dest == TSDataType.INT64 || dest == TSDataType.FLOAT || dest == TSDataType.DOUBLE) {
          return true;
        }
      case INT64:
        if (dest == TSDataType.DOUBLE) {
          return true;
        }
      case FLOAT:
        if (dest == TSDataType.DOUBLE) {
          return true;
        }
    }
    return false;
  }

  public static Object castValue(TSDataType srcDataType, TSDataType destDataType, Object value) {
    switch (srcDataType) {
      case INT32:
        if (destDataType == TSDataType.INT64) {
          value = (long) ((int) value);
        } else if (destDataType == TSDataType.FLOAT) {
          value = (float) ((int) value);
        } else if (destDataType == TSDataType.DOUBLE) {
          value = (double) ((int) value);
        }
        break;
      case INT64:
        if (destDataType == TSDataType.DOUBLE) {
          value = (double) ((long) value);
        }
        break;
      case FLOAT:
        if (destDataType == TSDataType.DOUBLE) {
          value = (double) ((float) value);
        }
        break;
    }
    return value;
  }

  public static Object castArray(TSDataType srcDataType, TSDataType destDataType, Object value) {
    switch (srcDataType) {
      case INT32:
        if (destDataType == TSDataType.INT64) {
          value = Arrays.stream((int[]) value).mapToLong(Long::valueOf).toArray();
        } else if (destDataType == TSDataType.FLOAT) {
          int[] tmp = (int[]) value;
          float[] result = new float[tmp.length];
          for (int i = 0; i < tmp.length; i++) {
            result[i] = (float) tmp[i];
          }
          value = result;
        } else if (destDataType == TSDataType.DOUBLE) {
          value = Arrays.stream((int[]) value).mapToDouble(Double::valueOf).toArray();
        }
        break;
      case INT64:
        if (destDataType == TSDataType.DOUBLE) {
          value = Arrays.stream((long[]) value).mapToDouble(Double::valueOf).toArray();
        }
        break;
      case FLOAT:
        if (destDataType == TSDataType.DOUBLE) {
          float[] tmp = (float[]) value;
          double[] result = new double[tmp.length];
          for (int i = 0; i < tmp.length; i++) {
            result[i] = tmp[i];
          }
          value = result;
        }
        break;
    }
    return value;
  }

  @TestOnly
  public static Object parseValueForTest(TSDataType dataType, String value)
      throws QueryProcessException {
    try {
      switch (dataType) {
        case BOOLEAN:
          return parseBoolean(value);
        case INT32:
          return Integer.parseInt(value);
        case INT64:
          return Long.parseLong(value);
        case FLOAT:
          return Float.parseFloat(value);
        case DOUBLE:
          return Double.parseDouble(value);
        case TEXT:
          return new Binary(value);
        default:
          throw new QueryProcessException("Unsupported data type:" + dataType);
      }
    } catch (NumberFormatException e) {
      throw new QueryProcessException(e.getMessage());
    }
  }

  private static boolean parseBoolean(String value) throws QueryProcessException {
    value = value.toLowerCase();
    if (SqlConstant.BOOLEAN_FALSE_NUM.equals(value) || SqlConstant.BOOLEAN_FALSE.equals(value)) {
      return false;
    }
    if (SqlConstant.BOOLEAN_TRUE_NUM.equals(value) || SqlConstant.BOOLEAN_TRUE.equals(value)) {
      return true;
    }
    throw new QueryProcessException("The BOOLEAN should be true/TRUE, false/FALSE or 0/1");
  }

  public static int runCli(
      List<Class<? extends Runnable>> commands,
      String[] args,
      String cliName,
      String cliDescription) {
    Cli.CliBuilder<Runnable> builder = Cli.builder(cliName);

    builder.withDescription(cliDescription).withDefaultCommand(Help.class).withCommands(commands);

    Cli<Runnable> parser = builder.build();

    int status = 0;
    try {
      Runnable parse = parser.parse(args);
      parse.run();
    } catch (IllegalArgumentException
        | IllegalStateException
        | ParseArgumentsMissingException
        | ParseArgumentsUnexpectedException
        | ParseOptionConversionException
        | ParseOptionMissingException
        | ParseOptionMissingValueException
        | ParseCommandMissingException
        | ParseCommandUnrecognizedException e) {
      badUse(e);
      status = 1;
    } catch (Exception e) {
      err(Throwables.getRootCause(e));
      status = 2;
    }
    return status;
  }

  private static void badUse(Exception e) {
    System.out.println("node-tool: " + e.getMessage());
    System.out.println("See 'node-tool help' or 'node-tool help <command>'.");
  }

  private static void err(Throwable e) {
    System.err.println("error: " + e.getMessage());
    System.err.println("-- StackTrace --");
    System.err.println(Throwables.getStackTraceAsString(e));
  }
}
