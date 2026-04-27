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

package org.apache.iotdb.session.subscription.util;

import org.apache.iotdb.rpc.subscription.exception.SubscriptionIdentifierSemanticException;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.read.common.parser.PathVisitor;

import java.util.Objects;

public class IdentifierUtils {

  /**
   * refer org.apache.iotdb.db.queryengine.plan.parser.ASTVisitor#parseIdentifier(java.lang.String)
   */
  public static String checkAndParseIdentifier(final String src) {
    if (Objects.isNull(src)) {
      throw new SubscriptionIdentifierSemanticException("null identifier is not supported");
    }
    if (src.isEmpty()) {
      throw new SubscriptionIdentifierSemanticException("empty identifier is not supported");
    }
    if (src.startsWith(TsFileConstant.BACK_QUOTE_STRING)
        && src.endsWith(TsFileConstant.BACK_QUOTE_STRING)) {
      return src.substring(1, src.length() - 1)
          .replace(TsFileConstant.DOUBLE_BACK_QUOTE_STRING, TsFileConstant.BACK_QUOTE_STRING);
    }
    checkIdentifier(src);
    return src;
  }

  private static void checkIdentifier(final String src) {
    if (!TsFileConstant.IDENTIFIER_PATTERN.matcher(src).matches()
        || PathVisitor.isRealNumber(src)) {
      throw new SubscriptionIdentifierSemanticException(
          String.format(
              "%s is illegal, identifier not enclosed with backticks can only consist of digits, characters and underscore.",
              src));
    }
  }
}
