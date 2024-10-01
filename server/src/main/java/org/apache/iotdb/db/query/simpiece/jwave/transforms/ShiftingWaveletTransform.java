/**
 * Shifting Wavelet Transform shifts a wavelet of smallest wavelength over the input array, then by
 * the double wavelength, .., and so on.
 *
 * @author Christian (graetz23@gmail.com)
 * @date 15.02.2016 23:12:55
 *     <p>ShiftingWaveletTransform.java
 */
package org.apache.iotdb.db.query.simpiece.jwave.transforms;

import org.apache.iotdb.db.query.simpiece.jwave.exceptions.JWaveException;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.Wavelet;

/**
 * Shifting Wavelet Transform shifts a wavelet of smallest wavelength over the input array, then by
 * the double wavelength, .., and so on.
 *
 * @author Christian (graetz23@gmail.com)
 * @date 15.02.2016 23:12:55
 */
public class ShiftingWaveletTransform extends WaveletTransform {

  /**
   * Constructor taking Wavelet object for performing the transform.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 15.02.2016 23:12:55
   */
  public ShiftingWaveletTransform(Wavelet wavelet) {
    super(wavelet);
  } // ShiftingWaveletTransform

  /**
   * Forward method that uses strictly the abilities of an orthogonal transform.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 15.02.2016 23:12:55 (non-Javadoc)
   * @see jwave.transforms.BasicTransform#forward(double[])
   */
  @Override
  public double[] forward(double[] arrTime) throws JWaveException {

    int length = arrTime.length;

    int div = 2;
    int odd = length % div; // if odd == 1 => steps * 2 + odd else steps * 2

    double[] arrHilb = new double[length];
    for (int i = 0; i < length; i++) arrHilb[i] = arrTime[i];

    while (div <= length) {

      int splits = length / div; // cuts the digits == round down to full

      // doing smallest wavelength of div by no of steps

      for (int s = 0; s < splits; s++) {

        double[] arrDiv = new double[div];
        double[] arrRes = null;

        for (int p = 0; p < div; p++) arrDiv[p] = arrHilb[s * div + p];

        arrRes = _wavelet.forward(arrDiv, div);

        for (int q = 0; q < div; q++) arrHilb[s * div + q] = arrRes[q];
      } // s

      div *= 2;
    } // while

    if (odd == 1) arrHilb[length - 1] = arrTime[length - 1];

    return arrHilb;
  } // forward

  /**
   * Reverse method that uses strictly the abilities of an orthogonal transform.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 15.02.2016 23:12:55 (non-Javadoc)
   * @see jwave.transforms.BasicTransform#reverse(double[])
   */
  @Override
  public double[] reverse(double[] arrHilb) throws JWaveException {

    int length = arrHilb.length;

    int div = 0;
    if (length % 2 == 0) div = length;
    else {
      div = length / 2; // 2 = 4.5 => 4
      div *= 2; // 4 * 2 = 8
    }

    int odd = length % div; // if odd == 1 => steps * 2 + odd else steps * 2

    double[] arrTime = new double[length];
    for (int i = 0; i < length; i++) arrTime[i] = arrHilb[i];

    while (div >= 2) {

      int splits = length / div; // cuts the digits == round down to full

      // doing smallest wavelength of div by no of steps

      for (int s = 0; s < splits; s++) {

        double[] arrDiv = new double[div];
        double[] arrRes = null;

        for (int p = 0; p < div; p++) arrDiv[p] = arrTime[s * div + p];

        arrRes = _wavelet.reverse(arrDiv, div);

        for (int q = 0; q < div; q++) arrTime[s * div + q] = arrRes[q];
      } // s

      div /= 2;
    } // while

    if (odd == 1) arrTime[length - 1] = arrHilb[length - 1];

    return arrTime;
  } // reverse
} // class
