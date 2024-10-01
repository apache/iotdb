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
package org.apache.iotdb.db.query.simpiece.jwave.transforms;

import org.apache.iotdb.db.query.simpiece.jwave.exceptions.JWaveException;
import org.apache.iotdb.db.query.simpiece.jwave.exceptions.JWaveFailure;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.Wavelet;

import java.util.Arrays;

/**
 * Base class for - stepping - forward and reverse methods, due to one kind of a Fast Wavelet Packet
 * Transform (WPT) or Wavelet Packet Decomposition (WPD) in 1-D using a specific Wavelet.
 *
 * @date 23.02.2010 13:44:05
 * @author Christian (graetz23@gmail.com)
 */
public class WaveletPacketTransform extends WaveletTransform {

  /**
   * Constructor receiving a Wavelet object and setting identifier of transform.
   *
   * @date 23.02.2010 13:44:05
   * @author Christian (graetz23@gmail.com)
   * @param wavelet object of type Wavelet
   */
  public WaveletPacketTransform(Wavelet wavelet) {

    super(wavelet);

    _name = "Wavelet Packet Transform";
  } // WaveletPacketTransform

  /**
   * Performs a 1-D forward transform from time domain to Hilbert domain using one kind of a Wavelet
   * Packet Transform (WPT) algorithm for a given array of dimension (length) 2^p | pEN; N = 2, 4,
   * 8, 16, 32, 64, 128, .., and so on. However, the algorithms stops for a supported level that has
   * be in the range 0, .., p of the dimension of the input array; 0 is the time series itself and p
   * is the maximal number of possible levels.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 22.03.2015 12:35:15
   * @throws JWaveException if givven array is not of length 2^p | pEN or the given level is out of
   *     range for the supported Hilbert space (array).
   * @see jwave.transforms.BasicTransform#forward(double[], int)
   */
  @Override
  public double[] forward(double[] arrTime, int level) throws JWaveException {

    if (!isBinary(arrTime.length))
      throw new JWaveFailure(
          "given array length is not 2^p | p E N ... = 1, 2, 4, 8, 16, 32, .. "
              + "please use the Ancient Egyptian Decomposition for any other array length!");

    int noOfLevels = calcExponent(arrTime.length);
    if (level < 0 || level > noOfLevels)
      throw new JWaveFailure(
          "WaveletPacketTransform#forward - given level is out of range for given array");

    double[] arrHilb = new double[arrTime.length];
    for (int i = 0; i < arrTime.length; i++) arrHilb[i] = arrTime[i];

    int k = arrTime.length;

    int h = arrTime.length;

    int transformWavelength = _wavelet.getTransformWavelength(); // 2, 4, 8, 16, 32, ...

    int l = 0;

    while (h >= transformWavelength && l < level) {

      int g = k / h; // 1 -> 2 -> 4 -> 8 -> ...

      for (int p = 0; p < g; p++) {

        double[] iBuf = new double[h];

        for (int i = 0; i < h; i++) iBuf[i] = arrHilb[i + (p * h)];

        double[] oBuf = _wavelet.forward(iBuf, h);

        for (int i = 0; i < h; i++) arrHilb[i + (p * h)] = oBuf[i];
      } // packets

      h = h >> 1;

      l++;
    } // levels

    return arrHilb;
  } // forward

  /**
   * Performs a 1-D reverse transform from Hilbert domain to time domain using one kind of a Wavelet
   * Packet Transform (WPT) algorithm for a given array of dimension (length) 2^p | pEN; N = 2, 4,
   * 8, 16, 32, 64, 128, .., and so on. However, the algorithms starts for at a supported level that
   * has be in the range 0, .., p of the dimension of the input array; 0 is the time series itself
   * and p is the maximal number of possible levels. The coefficients of the input array have to
   * match to the supported level.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 22.03.2015 12:38:23
   * @throws JWaveException (non-Javadoc)
   * @see jwave.transforms.BasicTransform#reverse(double[], int)
   */
  @Override
  public double[] reverse(double[] arrHilb, int level) throws JWaveException {

    if (!isBinary(arrHilb.length))
      throw new JWaveFailure(
          "given array length is not 2^p | p E N ... = 1, 2, 4, 8, 16, 32, .. "
              + "please use the Ancient Egyptian Decomposition for any other array length!");

    int noOfLevels = calcExponent(arrHilb.length);
    if (level < 0 || level > noOfLevels)
      throw new JWaveFailure(
          "WaveletPacketTransform#reverse - given level is out of range for given array");

    int length = arrHilb.length; // length of first Hilbert space
    double[] arrTime = Arrays.copyOf(arrHilb, length);

    int transformWavelength = _wavelet.getTransformWavelength(); // 2, 4, 8, 16, 32, ...

    int k = arrTime.length;

    int h = transformWavelength;

    int steps = calcExponent(length);
    for (int l = level; l < steps; l++)
      h = h << 1; // begin reverse transform at certain - matching - level of hilbert space

    while (h <= arrTime.length && h >= transformWavelength) {

      int g = k / h; // ... -> 8 -> 4 -> 2 -> 1

      for (int p = 0; p < g; p++) {

        double[] iBuf = new double[h];

        for (int i = 0; i < h; i++) iBuf[i] = arrTime[i + (p * h)];

        double[] oBuf = _wavelet.reverse(iBuf, h);

        for (int i = 0; i < h; i++) arrTime[i + (p * h)] = oBuf[i];
      } // packets

      h = h << 1;
    } // levels

    return arrTime;
  } // reverse
} // class
