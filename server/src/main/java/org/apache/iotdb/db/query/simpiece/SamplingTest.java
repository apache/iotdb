package org.apache.iotdb.db.query.simpiece;

import org.apache.iotdb.db.query.simpiece.jwave.Transform;
import org.apache.iotdb.db.query.simpiece.jwave.TransformBuilder;
import org.apache.iotdb.db.query.simpiece.jwave.tools.MathToolKit;

public class SamplingTest {

  public static void main(String[] args) {
    int samplingRate = 1024 * 1024; // sampling rate
    int noOfOscillations = 1024;

    // generate sampled (discrete) sine over 2 pi
    double[] arrTime = MathToolKit.createSineOscillation(samplingRate, noOfOscillations);

    Transform transform = TransformBuilder.create("Fast Wavelet Transform", "Haar");

    double[] arrHilb = transform.forward(arrTime);

    double[] arrReco = transform.reverse(arrHilb);

    //    assertArray(arrTime, arrReco, 1e-10);

    // generate sampled (discrete) sine over 2 pi
    arrTime = MathToolKit.createCosineOscillation(samplingRate, noOfOscillations);

    arrHilb = transform.forward(arrTime);

    arrReco = transform.reverse(arrHilb);

    //    assertArray(arrTime, arrReco, 1e-10);

    transform = TransformBuilder.create("Wavelet Packet Transform", "Haar");

    arrHilb = transform.forward(arrTime);

    arrReco = transform.reverse(arrHilb);

    //    assertArray(arrTime, arrReco, 1e-10);

    // generate sampled (discrete) sine over 2 pi
    arrTime = MathToolKit.createCosineOscillation(samplingRate, noOfOscillations);

    arrHilb = transform.forward(arrTime);

    arrReco = transform.reverse(arrHilb);

    //    assertArray(arrTime, arrReco, 1e-10);
  }
}
