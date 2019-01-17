/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
package org.apache.iotdb.db.sql.parse;

/**
 * AstNodeOrigin contains contextual information about the object from whose definition a particular
 * AstNode originated. For example, suppose a view v is defined as <code>select x+1 as y from
 * t</code>, and we're processing a query
 * <code>select v1.y from v as v1</code>, and there's a type-checking problem with the expression
 * <code>x+1</code> due
 * to an ALTER TABLE on t subsequent to the creation of v. Then, when reporting the error, we want
 * to provide the parser location with respect to the definition of v (rather than with respect to
 * the top-level query, since that represents a completely different "parser coordinate system").
 * <p> </p>
 * So, when expanding the definition of v while analyzing the top-level query, we tag each AstNode
 * with a reference to an AstNodeOrign describing v and its usage within the query.
 */

public class AstNodeOrigin {

  private final String objectType;
  private final String objectName;
  private final String objectDefinition;
  private final String usageAlias;
  private final AstNode usageNode;

  /**
   * Constructor of AstNodeOrigin.
   *
   * @param objectType object type
   * @param objectName object name
   * @param objectDefinition object definition
   * @param usageAlias usage alias
   * @param usageNode usage node
   */
  public AstNodeOrigin(String objectType, String objectName, String objectDefinition,
      String usageAlias,
      AstNode usageNode) {
    this.objectType = objectType;
    this.objectName = objectName;
    this.objectDefinition = objectDefinition;
    this.usageAlias = usageAlias;
    this.usageNode = usageNode;
  }

  /**
   * get object type.
   *
   * @return the type of the object from which an AstNode originated, e.g. "view".
   */
  public String getObjectType() {
    return objectType;
  }

  /**
   * get object name.
   *
   * @return the name of the object from which an AstNode originated, e.g. "v".
   */
  public String getObjectName() {
    return objectName;
  }

  /**
   * get object definition.
   *
   * @return the definition of the object from which an AstNode originated, e.g. <code>select x+1 as
   * y from t</code>.
   */
  public String getObjectDefinition() {
    return objectDefinition;
  }

  /**
   * get usage alias.
   *
   * @return the alias of the object from which an AstNode originated, e.g. "v1" (this can help with
   * debugging context-dependent expansions)
   */
  public String getUsageAlias() {
    return usageAlias;
  }

  /**
   * get usage node.
   *
   * @return the expression node triggering usage of an object from which an AstNode originated,
   * e.g. <code>v as v1</code> (this can help with debugging context-dependent expansions)
   */
  public AstNode getUsageNode() {
    return usageNode;
  }
}

// End AstNodeOrigin.java