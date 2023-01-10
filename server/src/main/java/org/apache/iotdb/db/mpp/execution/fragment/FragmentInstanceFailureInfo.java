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

package org.apache.iotdb.db.mpp.execution.fragment;

import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import com.google.common.collect.ImmutableList;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * This class is inspired by Trino <a
 * href="https://github.com/trinodb/trino/blob/master/core/trino-main/src/main/java/io/trino/execution/ExecutionFailureInfo.java">...</a>
 */
public class FragmentInstanceFailureInfo implements Serializable {
  private static final Pattern STACK_TRACE_PATTERN =
      Pattern.compile("(.*)\\.(.*)\\(([^:]*)(?::(.*))?\\)");
  private final String message;
  private final FragmentInstanceFailureInfo cause;
  private final List<FragmentInstanceFailureInfo> suppressed;
  private final List<String> stack;

  public FragmentInstanceFailureInfo(
      String message,
      FragmentInstanceFailureInfo cause,
      List<FragmentInstanceFailureInfo> suppressed,
      List<String> stack) {
    requireNonNull(suppressed, "suppressed is null");
    requireNonNull(stack, "stack is null");

    this.message = message;
    this.cause = cause;
    this.suppressed = ImmutableList.copyOf(suppressed);
    this.stack = ImmutableList.copyOf(stack);
  }

  public String getMessage() {
    return message;
  }

  public FragmentInstanceFailureInfo getCause() {
    return cause;
  }

  public List<FragmentInstanceFailureInfo> getSuppressed() {
    return suppressed;
  }

  public List<String> getStack() {
    return stack;
  }

  public RuntimeException toException() {
    return toException(this);
  }

  public static FragmentInstanceFailureInfo toFragmentInstanceFailureInfo(Throwable throwable) {
    if (throwable == null) {
      return null;
    }
    return new FragmentInstanceFailureInfo(
        throwable.getMessage(),
        toFragmentInstanceFailureInfo(throwable.getCause()),
        Arrays.stream(throwable.getSuppressed())
            .map(FragmentInstanceFailureInfo::toFragmentInstanceFailureInfo)
            .collect(Collectors.toList()),
        Arrays.stream(throwable.getStackTrace()).map(Objects::toString).collect(toImmutableList()));
  }

  private static FailureException toException(FragmentInstanceFailureInfo failureInfo) {
    if (failureInfo == null) {
      return null;
    }
    FailureException failure =
        new FailureException(failureInfo.getMessage(), toException(failureInfo.getCause()));
    for (FragmentInstanceFailureInfo suppressed : failureInfo.getSuppressed()) {
      failure.addSuppressed(toException(suppressed));
    }
    ImmutableList.Builder<StackTraceElement> stackTraceBuilder = ImmutableList.builder();
    for (String stack : failureInfo.getStack()) {
      stackTraceBuilder.add(toStackTraceElement(stack));
    }
    ImmutableList<StackTraceElement> stackTrace = stackTraceBuilder.build();
    failure.setStackTrace(stackTrace.toArray(new StackTraceElement[0]));
    return failure;
  }

  public static StackTraceElement toStackTraceElement(String stack) {
    Matcher matcher = STACK_TRACE_PATTERN.matcher(stack);
    if (matcher.matches()) {
      String declaringClass = matcher.group(1);
      String methodName = matcher.group(2);
      String fileName = matcher.group(3);
      int number = -1;
      if (fileName.equals("Native Method")) {
        fileName = null;
        number = -2;
      } else if (matcher.group(4) != null) {
        number = Integer.parseInt(matcher.group(4));
      }
      return new StackTraceElement(declaringClass, methodName, fileName, number);
    }
    return new StackTraceElement("Unknown", stack, null, -1);
  }

  // region serialize && deserialize

  public ByteBuffer serialize() throws IOException {
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
    serialize(outputStream);
    return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
  }

  public void serialize(DataOutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(message, outputStream);
    if (cause == null) {
      ReadWriteIOUtils.write(0, outputStream);
    } else {
      ReadWriteIOUtils.write(1, outputStream);
      cause.serialize(outputStream);
    }
    ReadWriteIOUtils.write(suppressed.size(), outputStream);
    for (FragmentInstanceFailureInfo failureInfo : suppressed) {
      failureInfo.serialize(outputStream);
    }
    ReadWriteIOUtils.write(stack.size(), outputStream);
    for (String s : stack) {
      ReadWriteIOUtils.write(s, outputStream);
    }
  }

  public static FragmentInstanceFailureInfo deserialize(ByteBuffer byteBuffer) {
    String message = ReadWriteIOUtils.readString(byteBuffer);
    FragmentInstanceFailureInfo cause;
    List<FragmentInstanceFailureInfo> suppressed = new ArrayList<>();
    List<String> stack = new ArrayList<>();
    int flag = ReadWriteIOUtils.readInt(byteBuffer);
    if (flag == 0) {
      cause = null;
    } else {
      cause = deserialize(byteBuffer);
    }
    int suppressedSize = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < suppressedSize; i++) {
      suppressed.add(deserialize(byteBuffer));
    }
    int stackSize = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < stackSize; i++) {
      stack.add(ReadWriteIOUtils.readString(byteBuffer));
    }
    return new FragmentInstanceFailureInfo(message, cause, suppressed, stack);
  }

  // end region

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FragmentInstanceFailureInfo that = (FragmentInstanceFailureInfo) o;
    return (this.getMessage() == null
            ? that.getMessage() == null
            : this.getMessage().equals(that.getMessage()))
        && (this.getCause() == null
            ? that.getCause() == null
            : this.getCause().equals(that.getCause()))
        && this.getSuppressed().equals(that.getSuppressed())
        && this.getStack().equals(that.getStack());
  }

  private static class FailureException extends RuntimeException {
    FailureException(String message, FailureException cause) {
      super(message, cause);
    }
  }
}
