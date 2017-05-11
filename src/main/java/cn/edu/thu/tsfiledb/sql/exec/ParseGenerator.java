package cn.edu.thu.tsfiledb.sql.exec;

import cn.edu.thu.tsfiledb.sql.parse.ASTNode;
import cn.edu.thu.tsfiledb.sql.parse.ParseDriver;
import cn.edu.thu.tsfiledb.sql.parse.ParseException;

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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/**
 * ParseContextGenerator is a class that offers methods to generate ASTNode Tree
 *
 */
public final class ParseGenerator {

  /**
   * Parse the input {@link String} command and generate an ASTNode Tree.
   * @param command
   */
  public static ASTNode generateAST(//QueryState queryState,
      String command) throws ParseException {
   // Context ctx = new Context(queryState.getConf());
    ParseDriver pd = new ParseDriver();
    //ASTNode tree = pd.parse(command, ctx);
    ASTNode tree = pd.parse(command);
   // tree = ParseUtils.findRootNonNullToken(tree);
    
    return tree;
   }

}
