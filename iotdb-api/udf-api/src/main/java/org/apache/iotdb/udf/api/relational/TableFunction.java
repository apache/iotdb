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

package org.apache.iotdb.udf.api.relational;

import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.relational.table.TableFunctionAnalysis;
import org.apache.iotdb.udf.api.relational.table.TableFunctionHandle;
import org.apache.iotdb.udf.api.relational.table.TableFunctionProcessorProvider;
import org.apache.iotdb.udf.api.relational.table.argument.Argument;
import org.apache.iotdb.udf.api.relational.table.argument.ScalarArgument;
import org.apache.iotdb.udf.api.relational.table.argument.TableArgument;
import org.apache.iotdb.udf.api.relational.table.specification.ParameterSpecification;
import org.apache.iotdb.udf.api.relational.table.specification.ScalarParameterSpecification;
import org.apache.iotdb.udf.api.relational.table.specification.TableParameterSpecification;

import java.util.List;
import java.util.Map;

/**
 * The result relation type of the table function consists of:
 *
 * <ul>
 *   <li>1. columns created by the table function, called the proper columns.
 *   <li>2. passed columns from input tables:
 *       <ul>
 *         <li>- for tables with the "pass through columns" option, these are all columns of the
 *             table,
 *         <li>- for tables without the "pass through columns" option, these are the partitioning
 *             columns of the table, if any.
 *       </ul>
 * </ul>
 */
public interface TableFunction extends SQLFunction {

  /**
   * This method is used to define the specification of the arguments required by the table
   * function. Each argument is described using a {@link ParameterSpecification} object, which
   * encapsulates details such as the argument name, whether it is required, its default value (if
   * any), etc.
   *
   * <p>The {@link ParameterSpecification} class is abstract and has two concrete implementations:
   *
   * <ul>
   *   <li>{@link TableParameterSpecification}: Used for table parameter specification.We only
   *       support at most one table parameter.
   *   <li>{@link ScalarParameterSpecification}: Used for scalar parameter specification. We support
   *       any number of scalar parameters.
   * </ul>
   *
   * @return a list of {@link ParameterSpecification} objects describing the function's arguments.
   *     The list should include all parameters that the table function expects, along with their
   *     constraints and default values (if applicable).
   */
  List<ParameterSpecification> getArgumentsSpecifications();

  /**
   * This method is responsible for analyzing the provided arguments and constructing a {@link
   * TableFunctionAnalysis} object in runtime. During analysis, the method should:
   *
   * <ul>
   *   <li>Extract values from scalar arguments (instances of {@link ScalarArgument}) or extract
   *       table descriptions from table arguments (instances of {@link TableArgument}).
   *   <li>Construct a {@link TableFunctionAnalysis} object that contains:
   *       <ul>
   *         <li>A description of proper columns.
   *         <li>A map indicating which columns from the input table arguments are required for the
   *             function to execute.
   *         <li>A TableFunctionExecutionInfo which stores all information necessary to execute the
   *             table function.
   *       </ul>
   * </ul>
   *
   * @param arguments a map of argument names to their corresponding {@link Argument} values. The
   *     keys should match the parameter names defined in {@link #getArgumentsSpecifications()}.
   * @throws UDFException if any argument is invalid or if an error occurs during analysis
   * @return a {@link TableFunctionAnalysis} object containing the analysis result
   */
  TableFunctionAnalysis analyze(Map<String, Argument> arguments) throws UDFException;

  TableFunctionHandle createTableFunctionHandle();

  /**
   * This method is used to obtain a {@link TableFunctionProcessorProvider} that will be responsible
   * for creating processors to handle the transformation of input data into output table. The
   * provider is initialized with the validated arguments.
   *
   * @param tableFunctionHandle the object containing the execution information, which is generated
   *     in the {@link TableFunction#analyze} process.
   * @return a {@link TableFunctionProcessorProvider} for creating processors
   */
  TableFunctionProcessorProvider getProcessorProvider(TableFunctionHandle tableFunctionHandle);
}
