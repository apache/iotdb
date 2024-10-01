/**
 * JWave is distributed under the MIT License (MIT); this file is part of.
 *
 * <p>Copyright (c) 2008-2024 Christian (graetz23@gmail.com)
 *
 * <p>Permission is hereby granted, free of charge, to any person obtaining a copy of this software
 * and associated documentation files (the "Software"), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge, publish, distribute,
 * sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * <p>The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 *
 * <p>THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING
 * BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package org.apache.iotdb.db.query.simpiece;

import org.apache.iotdb.db.query.simpiece.jwave.Transform;
import org.apache.iotdb.db.query.simpiece.jwave.TransformBuilder;
import org.apache.iotdb.db.query.simpiece.jwave.compressions.Compressor;
import org.apache.iotdb.db.query.simpiece.jwave.compressions.CompressorMagnitude;
import org.apache.iotdb.db.query.simpiece.jwave.tools.MathToolKit;
import org.apache.iotdb.tsfile.read.common.IOMonitor2;

import java.io.IOException;

public class MySample_JWaveTest {

  public static void main(String[] args) throws IOException {
    int samplingRate = 1024; // sampling rate
    int noOfOscillations = 1024;
    // generate sampled (discrete) sine over 2 pi
    double[] arrTime = MathToolKit.createSineOscillation(samplingRate, noOfOscillations);

    //    Transform transform = TransformBuilder.create("Fast Wavelet Transform", "Haar");
    Transform transform = TransformBuilder.create("Discrete Fourier Transform", "Haar");

    double[] arrHilb = transform.forward(arrTime);

    Compressor compressor = new CompressorMagnitude(0.000005);

    double[] arrComp = compressor.compress(arrHilb);

    //    PrintWriter printWriter = new PrintWriter("tmp.csv");
    //    for (int i = 0; i < arrComp.length; i++) {
    //      printWriter.println(arrHilb[i] + "," + arrComp[i]);
    //    }
    //    printWriter.close();

    double[] arrReco = transform.reverse(arrComp);

    // calculate and print the absolute difference
    int pos = 0;
    double maxDiff = 0.;
    for (int i = 0; i < arrReco.length; i++) {
      double diff = Math.abs(arrTime[i] - arrReco[i]);
      if (diff > maxDiff) {
        maxDiff = diff;
        pos = i;
      } // if
    } // i
    System.out.println("absolute max difference at position " + pos + " is: " + maxDiff);

    // calculate the compression rate
    double compressionRate = compressor.calcCompressionRate(arrComp);
    System.out.println("The achieved compression rate: " + compressionRate);

    System.out.println();

    System.out.println(IOMonitor2.DCP_D_getAllSatisfiedPageData_traversedPointNum);
  }
}
