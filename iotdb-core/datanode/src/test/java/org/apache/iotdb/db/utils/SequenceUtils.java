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
package org.apache.iotdb.db.utils;

import java.util.Random;

public class SequenceUtils {
  public interface DoubleSequenceGenerator {
    double gen(int id);
  }

  public interface DoubleSequenceGeneratorFactory {
    DoubleSequenceGenerator create();
  }

  public static class SimpleDoubleSequenceGenerator implements DoubleSequenceGenerator {

    @Override
    public double gen(int id) {
      return id * 1.0;
    }

    public static class Factory implements DoubleSequenceGeneratorFactory{
      @Override
      public DoubleSequenceGenerator create() {
        return new SimpleDoubleSequenceGenerator();
      }
    }
  }

  public static class UniformDoubleSequenceGenerator implements DoubleSequenceGenerator {

    private Random random = new Random();
    @Override
    public double gen(int id) {
      return random.nextDouble();
    }

    public static class Factory implements DoubleSequenceGeneratorFactory{
      @Override
      public DoubleSequenceGenerator create() {
        return new UniformDoubleSequenceGenerator();
      }
    }
  }

  public static class GaussianDoubleSequenceGenerator implements DoubleSequenceGenerator {

    private Random random = new Random();
    @Override
    public double gen(int id) {
      return random.nextGaussian();
    }

    public static class Factory implements DoubleSequenceGeneratorFactory{
      @Override
      public DoubleSequenceGenerator create() {
        return new GaussianDoubleSequenceGenerator();
      }
    }
  }
}
