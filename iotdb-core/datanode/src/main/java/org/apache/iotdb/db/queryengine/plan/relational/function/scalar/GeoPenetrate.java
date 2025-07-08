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

package org.apache.iotdb.db.queryengine.plan.relational.function.scalar;

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.utils.model.ModelReader;
import org.apache.iotdb.udf.api.customizer.analysis.ScalarFunctionAnalysis;
import org.apache.iotdb.udf.api.customizer.parameter.FunctionArguments;
import org.apache.iotdb.udf.api.exception.UDFArgumentNotValidException;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.relational.ScalarFunction;
import org.apache.iotdb.udf.api.relational.access.Record;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.utils.ObjectTypeUtils.getObjectPathFromBinary;

public class GeoPenetrate implements ScalarFunction {

  private ModelReader reader;
  private List<List<Integer>> startAndEndTimeArray;

  @Override
  public ScalarFunctionAnalysis analyze(FunctionArguments arguments)
      throws UDFArgumentNotValidException {
    if (arguments.getArgumentsSize() != 2 && arguments.getArgumentsSize() != 3) {
      throw new UDFArgumentNotValidException(
          "function requires 2 or 3 arguments, current parameters' size is "
              + arguments.getArgumentsSize());
    }
    if (arguments.getDataType(0) != Type.OBJECT) {
      throw new UDFArgumentNotValidException("function's first parameter type should be OBJECT");
    }

    if (arguments.getDataType(1) != Type.STRING) {
      throw new UDFArgumentNotValidException("function's second parameter type should be STRING");
    }

    if (arguments.getArgumentsSize() == 3 && arguments.getDataType(1) != Type.STRING) {
      throw new UDFArgumentNotValidException("function's third parameter type should be STRING");
    }

    return new ScalarFunctionAnalysis.Builder().outputDataType(Type.BLOB).build();
  }

  @Override
  public void beforeStart(FunctionArguments arguments) throws UDFException {
    if (arguments.getArgumentsSize() == 2) {
      reader = ModelReader.getDefaultInstance();
    }
  }

  @Override
  public Object evaluate(Record input) throws UDFException {
    if (reader == null) {
      reader = ModelReader.getInstance(input.getString(2));
    }
    if (startAndEndTimeArray == null) {
      startAndEndTimeArray = getStartAndEndTimeArray(input.getString(1));
    }
    if (input.isNull(0)) {
      return null;
    } else {
      File objectPath = getObjectPathFromBinary(input.getBinary(0));
      List<float[]> res = reader.penetrate(objectPath.getAbsolutePath(), startAndEndTimeArray);
      int count = 0;
      for (float[] array : res) {
        count += array.length;
      }
      ByteBuffer byteBuffer = ByteBuffer.allocate(count * Float.BYTES);
      for (float[] array : res) {
        for (float value : array) {
          ReadWriteIOUtils.write(value, byteBuffer);
        }
      }
      return new Binary(byteBuffer.array());
    }
  }

  // argument like: 1,2;200,201;300,401
  private List<List<Integer>> getStartAndEndTimeArray(String argument) {
    String[] arguments = argument.split(",");
    if (arguments.length % 2 != 0) {
      throw new SemanticException("second argument is wrong: " + argument);
    }
    List<List<Integer>> res = new ArrayList<>(arguments.length);
    for (int index = 0; index < arguments.length; index += 2) {
      List<Integer> startAndEndTime = new ArrayList<>(2);
      try {
        startAndEndTime.add(Integer.parseInt(arguments[index]));
        startAndEndTime.add(Integer.parseInt(arguments[index + 1]));
      } catch (NumberFormatException e) {
        throw new SemanticException("second argument is wrong: " + argument);
      }
      res.add(startAndEndTime);
    }
    return res;
  }
}
