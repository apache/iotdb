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

import org.jtransforms.fft.DoubleFFT_1D;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TableFFTTest {

	@Test
	public void testSineWavePeakFrequency() {
		int n = 8;
		double[] a = new double[2 * n];
		for (int i = 0; i < n; i++) {
			a[2 * i] = Math.sin(2 * Math.PI * i / (double) n);
			a[2 * i + 1] = 0;
		}

		DoubleFFT_1D fft = new DoubleFFT_1D(n);
		fft.complexForward(a);

		// find peak bin
		int maxIdx = 0;
		double maxAbs = 0;
		for (int i = 0; i < n / 2; i++) {
			double abs = Math.sqrt(a[2 * i] * a[2 * i] + a[2 * i + 1] * a[2 * i + 1]);
			if (abs > maxAbs) {
				maxAbs = abs;
				maxIdx = i;
			}
		}
		// sine wave with period n should peak at bin 1
		assertEquals(1, maxIdx);
	}

	@Test
	public void testDCComponent() {
		int n = 4;
		double[] a = new double[2 * n];
		// constant signal → all energy at bin 0
		for (int i = 0; i < n; i++) {
			a[2 * i] = 1.0;
			a[2 * i + 1] = 0;
		}

		DoubleFFT_1D fft = new DoubleFFT_1D(n);
		fft.complexForward(a);

		double dc = Math.sqrt(a[0] * a[0] + a[1] * a[1]);
		assertEquals(4.0, dc, 1e-9);
	}
}