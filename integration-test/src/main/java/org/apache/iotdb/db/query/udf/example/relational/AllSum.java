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

package org.apache.iotdb.db.query.udf.example.relational;

import org.apache.iotdb.udf.api.customizer.parameter.FunctionParameters;
import org.apache.iotdb.udf.api.exception.UDFParameterNotValidException;
import org.apache.iotdb.udf.api.relational.ScalarFunction;
import org.apache.iotdb.udf.api.relational.access.Record;
import org.apache.iotdb.udf.api.type.Type;

/**
 * Calculate the sum of all parameters. Only support inputs of INT32,INT64,DOUBLE,FLOAT type.
 */
public class AllSum implements ScalarFunction {

    @Override
    public void validate(FunctionParameters parameters) throws Exception {
        if (parameters.getChildExpressionsSize() < 1) {
            throw new UDFParameterNotValidException("At least one parameter is required.");
        }
        for(int i = 0; i < parameters.getChildExpressionsSize(); i++) {
            if(parameters.getDataType(i)!=Type.INT32 && parameters.getDataType(i)!=Type.INT64&&parameters.getDataType(i)!=Type.FLOAT&&parameters.getDataType(i)!=Type.DOUBLE){
                throw new UDFParameterNotValidException("Only support inputs of INT32,INT64,DOUBLE,FLOAT type.");
            }
        }
    }

    @Override
    public Type inferOutputType(FunctionParameters parameters) {
        return Type.DOUBLE;
    }

    @Override
    public Object evaluate(Record input) throws Exception {
        double res = 0;
        for (int i = 0; i < input.size(); i++) {
            if(!input.isNull(i)) {
                switch (input.getDataType(i)) {
                    case INT32:
                        res += input.getInt(i);
                        break;
                    case INT64:
                        res += input.getLong(i);
                        break;
                    case FLOAT:
                        res += input.getFloat(i);
                        break;
                    case DOUBLE:
                        res += input.getDouble(i);
                        break;
                }
            }
        }
        return res;
    }
}
