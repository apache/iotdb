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

package org.apache.iotdb.db.pipe.processor.aggregate.operator.intermediateresult;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Pair;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * The class that define the calculation and ser/de logic of an intermediate result. There shall be
 * a one-to-one match between an intermediate result's operator and its name.
 *
 * <p>The {@link IntermediateResultOperator}'s update function is called once per point, thus it
 * shall implement update functions with primitive type input and shall handle the intermediate
 * value itself in order to better optimize the calculation and save resource.
 *
 * <p>Besides, the variables can also be passed in by the instantiating function, to help
 * customizing calculation.
 */
public interface IntermediateResultOperator {

  /**
   * Return the name of the operator, the name shall be in lower case
   *
   * @return the name of the operator
   */
  String getName();

  /**
   * The system will pass in some parameters (e.g. timestamp precision) to help the inner function
   * correctly operate. This will be called at the very first of the operator's lifecycle.
   *
   * @param systemParams the system parameters
   */
  void configureSystemParameters(Map<String, String> systemParams);

  /**
   * The operator may init its value and typically set the output intermediate value type by the
   * input data type. The output value type is arbitrary, such as a List, a Map, or any type that is
   * needed in aggregation value calculation. You can even hold all the points in the output value
   * and hand it to the aggregation function.
   *
   * <p>If the input type is unsupported, return {@code false}
   *
   * @param initialInput the initial data
   * @return if the input type is supported
   */
  boolean initAndGetIsSupport(boolean initialInput, long initialTimestamp);

  boolean initAndGetIsSupport(int initialInput, long initialTimestamp);

  boolean initAndGetIsSupport(long initialInput, long initialTimestamp);

  boolean initAndGetIsSupport(float initialInput, long initialTimestamp);

  boolean initAndGetIsSupport(double initialInput, long initialTimestamp);

  boolean initAndGetIsSupport(String initialInput, long initialTimestamp);

  /**
   * Use the input to update the intermediate result. The input is all raw types instead of Object
   * to avoid the boxing and unboxing operations' resource occupation.
   *
   * @param input the inputs
   */
  void updateValue(boolean input, long timestamp);

  void updateValue(int input, long timestamp);

  void updateValue(long input, long timestamp);

  void updateValue(float input, long timestamp);

  void updateValue(double input, long timestamp);

  void updateValue(String input, long timestamp);

  /**
   * Get the result and its type to calculate the aggregated value. If the type is List, Map or
   * other non-included types please use {@link TSDataType#UNKNOWN}.
   */
  Pair<TSDataType, Object> getResult();

  /**
   * Serialize its intermediate result to the outputStream to allow shutdown restart. The operator
   * may as well serialize its own status.
   */
  void serialize(DataOutputStream outputStream) throws IOException;

  /** Deserialize the object from byteBuffer to allow shutdown restart */
  void deserialize(ByteBuffer byteBuffer) throws IOException;
}
