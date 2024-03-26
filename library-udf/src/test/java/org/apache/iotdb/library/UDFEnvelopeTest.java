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

package org.apache.iotdb.library;

import org.apache.iotdb.library.frequency.UDFEnvelopeAnalysis;

import org.junit.Assert;
import org.junit.Test;

public class UDFEnvelopeTest {
  private final UDFEnvelopeAnalysis analysis = new UDFEnvelopeAnalysis();

  @Test
  public void testLinearIncreasing() {
    double[] expectedEnvelope = {6.2844, 1.5582, 0.8503, 0.5128, 0.2636};
    double[] vibData = new double[10];
    for (int i = 0; i < 10; i++) vibData[i] = i + 1;
    double[] envelope = analysis.envelopeAnalyze(vibData);
    Assert.assertArrayEquals(expectedEnvelope, envelope, 0.01);
  }

  @Test
  public void testLinearDecreasing() {
    double[] expectedEnvelope = {6.2844, 1.5582, 0.8503, 0.5128, 0.2636};
    double[] vibData = new double[10];
    for (int i = 0; i < 10; i++) vibData[i] = 10 - i;
    double[] envelope = analysis.envelopeAnalyze(vibData);
    Assert.assertArrayEquals(expectedEnvelope, envelope, 0.01);
  }

  @Test
  public void testQuadratic() {
    double[] expectedEnvelope = {50.0465, 19.4957, 10.2427, 5.9496, 2.0566};
    double[] vibData = new double[10];
    for (int i = 0; i < 10; i++) vibData[i] = Math.pow(i + 1, 2);
    double[] envelope = analysis.envelopeAnalyze(vibData);
    Assert.assertArrayEquals(expectedEnvelope, envelope, 0.01);
  }

  @Test
  public void testSineWave() {
    double[] expectedEnvelope = {0.9379, 0.0738, 0.0457, 0.0394, 0.0203};
    double[] vibData = new double[10];
    for (int i = 0; i < 10; i++) vibData[i] = Math.sin(i * 2 * Math.PI / 9);
    double[] envelope = analysis.envelopeAnalyze(vibData);
    Assert.assertArrayEquals(expectedEnvelope, envelope, 0.01);
  }

  @Test
  public void testCosineWave() {
    double[] expectedEnvelope = {1.0437, 0.0097, 0.0163, 0.0056, 0.0005};
    double[] vibData = new double[10];
    for (int i = 0; i < 10; i++) vibData[i] = Math.cos(i * 2 * Math.PI / 9);
    double[] envelope = analysis.envelopeAnalyze(vibData);
    Assert.assertArrayEquals(expectedEnvelope, envelope, 0.01);
  }

  @Test
  public void testExponential() {
    double[] expectedEnvelope = {6789.997, 4310.316, 2098.5, 1531.1508, 441.3};
    double[] vibData = new double[10];
    for (int i = 0; i < 10; i++) vibData[i] = Math.exp(i + 1);
    double[] envelope = analysis.envelopeAnalyze(vibData);
    Assert.assertArrayEquals(expectedEnvelope, envelope, 0.01);
  }

  @Test
  public void testLogarithmic() {
    double[] expectedEnvelope = {1.7149, 0.2899, 0.1784, 0.1201, 0.0943};
    double[] vibData = new double[10];
    for (int i = 0; i < 10; i++) vibData[i] = Math.log(i + 1);
    double[] envelope = analysis.envelopeAnalyze(vibData);
    Assert.assertArrayEquals(expectedEnvelope, envelope, 0.01);
  }

  @Test
  public void testAmplitudeModulatedSineWave() {
    double[] expectedEnvelope = {5.1938, 1.3139, 0.3249, 0.2471, 0.1196};
    double[] vibData = new double[10];
    for (int i = 0; i < 10; i++) vibData[i] = (i + 1) * Math.sin(i * 2 * Math.PI / 9);
    double[] envelope = analysis.envelopeAnalyze(vibData);
    Assert.assertArrayEquals(expectedEnvelope, envelope, 0.01);
  }

  @Test
  public void testDampedSineWave() {
    double[] expectedEnvelope = {0.2345, 0.0824, 0.0207, 0.0161, 0.0075};
    double[] vibData = new double[10];
    for (int i = 0; i < 10; i++)
      vibData[i] = Math.exp(-0.3 * (i + 1)) * Math.sin(i * 2 * Math.PI / 9);
    double[] envelope = analysis.envelopeAnalyze(vibData);
    Assert.assertArrayEquals(expectedEnvelope, envelope, 0.01);
  }

  @Test
  public void testCompositeWaveform() {
    double[] expectedEnvelope = {1.2908, 0.3926, 0.0535, 0.0570, 0.0308};
    double[] vibData = new double[10];
    for (int i = 0; i < 10; i++)
      vibData[i] = Math.sin(i * 2 * Math.PI / 9) + Math.cos(i * 4 * Math.PI / 9);
    double[] envelope = analysis.envelopeAnalyze(vibData);
    Assert.assertArrayEquals(expectedEnvelope, envelope, 0.01);
  }
}
