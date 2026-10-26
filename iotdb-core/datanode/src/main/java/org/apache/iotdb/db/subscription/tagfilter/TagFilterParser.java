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

package org.apache.iotdb.db.subscription.tagfilter;

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.parser.ParsingException;
import org.apache.iotdb.db.i18n.DataNodeMiscMessages;
import org.apache.iotdb.db.subscription.columnfilter.ColumnFilterParser;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;

import java.nio.charset.StandardCharsets;

/** Parses the restricted subscription filter expression syntax for table-model TAG values. */
public class TagFilterParser {

  private static final int MAX_UTF8_BYTES = 4096;

  private final ColumnFilterParser expressionParser = new ColumnFilterParser();

  public Expression parse(final String rawTagFilter) throws SubscriptionException {
    try {
      if (rawTagFilter == null || rawTagFilter.trim().isEmpty()) {
        throw new IllegalArgumentException(
            DataNodeMiscMessages.EXCEPTION_TAG_FILTER_SHOULD_NOT_BE_EMPTY_507CA5B0);
      }
      validateText(rawTagFilter);
      return expressionParser.parse(rawTagFilter);
    } catch (final ParsingException | IllegalArgumentException e) {
      throw new SubscriptionException(
          String.format(
              DataNodeMiscMessages.EXCEPTION_INVALID_TAG_FILTER_ARG_E4B1C1C6, e.getMessage()),
          e);
    }
  }

  public Expression parseAndValidate(final String rawTagFilter) throws SubscriptionException {
    final Expression expression = parse(rawTagFilter);
    try {
      TagFilterValidator.validate(expression);
    } catch (final IllegalArgumentException e) {
      throw new SubscriptionException(
          String.format(
              DataNodeMiscMessages.EXCEPTION_INVALID_TAG_FILTER_ARG_E4B1C1C6, e.getMessage()),
          e);
    }
    return expression;
  }

  private static void validateText(final String text) {
    if (text.getBytes(StandardCharsets.UTF_8).length > MAX_UTF8_BYTES) {
      throw new IllegalArgumentException(
          String.format(
              DataNodeMiscMessages
                  .EXCEPTION_TAG_FILTER_EXCEEDS_MAXIMUM_UTF_8_LENGTH_OF_ARG_BYTES_9C8400F2,
              MAX_UTF8_BYTES));
    }
    for (int i = 0; i < text.length(); i++) {
      final char current = text.charAt(i);
      if (Character.isISOControl(current) && !Character.isWhitespace(current)) {
        throw new IllegalArgumentException(
            String.format(
                DataNodeMiscMessages.UNEXPECTED_CHARACTER_FMT, Integer.toHexString(current)));
      }
      if (Character.isHighSurrogate(current)) {
        if (i + 1 >= text.length() || !Character.isLowSurrogate(text.charAt(++i))) {
          throw new IllegalArgumentException(
              String.format(
                  DataNodeMiscMessages.UNEXPECTED_CHARACTER_FMT, Integer.toHexString(current)));
        }
      } else if (Character.isLowSurrogate(current)) {
        throw new IllegalArgumentException(
            String.format(
                DataNodeMiscMessages.UNEXPECTED_CHARACTER_FMT, Integer.toHexString(current)));
      }
    }
  }
}
