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
package org.apache.iotdb.commons.path.fa;

import org.apache.iotdb.commons.path.fa.dfa.PatternDFA;
import org.apache.iotdb.commons.path.fa.nfa.SimpleNFA;

public class FAFactory {

  //    private LoadingCache<PartialPath, IMNode> mNodeCache;

  public IPatternFA constructDFA(IPatternFA.Builder builder) {
    return new PatternDFA(builder.getPathPattern(), builder.isPrefixMatch());
  }

  public IPatternFA constructNFA(IPatternFA.Builder builder) {
    return new SimpleNFA(builder.getPathPattern(), builder.isPrefixMatch());
  }

  private FAFactory() {}

  public static FAFactory getInstance() {
    return FAFactoryHolder.INSTANCE;
  }

  /** singleton pattern. */
  private static class FAFactoryHolder {
    private static final FAFactory INSTANCE = new FAFactory();
  }
}
