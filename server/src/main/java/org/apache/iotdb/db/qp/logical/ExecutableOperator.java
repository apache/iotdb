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
package org.apache.iotdb.db.qp.logical;

import org.apache.iotdb.db.qp.logical.crud.FilterOperator;
import org.apache.iotdb.db.qp.logical.crud.FromOperator;
import org.apache.iotdb.db.qp.logical.crud.SetPathOperator;
import org.apache.iotdb.tsfile.read.common.Path;

import java.util.List;

/**
 * ExecutableOperator indicates a line of executable statement, including insert,
 * update, delete and query.
 */
public abstract class ExecutableOperator extends Operator {

  public ExecutableOperator(int tokenIntType) {
    super(tokenIntType);
  }

  public SetPathOperator getSetPathOperator(){
    return null;
  }

  public boolean setSetPathOperator(SetPathOperator setPathOperator){
    return false;
  }

  public FilterOperator getFilterOperator(){
    return null;
  }

  public boolean setFilterOperator(FilterOperator filterOperator){
    return false;
  }

  public FromOperator getFromOperator(){
    return null;
  }

  public boolean setFromOperator(FromOperator fromOperator){
    return false;
  }

  public List<Path> getSelectedPaths() {
    List<Path> suffixPaths = null;
    SetPathOperator setPathOperator = getSetPathOperator();
    if (setPathOperator != null) {
      suffixPaths = setPathOperator.getSuffixPaths();
    }
    return suffixPaths;
  }

}
