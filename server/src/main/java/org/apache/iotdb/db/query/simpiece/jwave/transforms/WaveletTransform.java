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

/**
 * @author Christian (graetz23@gmail.com)
 * @date 15.02.2014 21:05:33
 */
public abstract class WaveletTransform extends BasicTransform {

  /**
   * The used wavelet for transforming
   *
   * @author Christian (graetz23@gmail.com)
   * @date 15.02.2014 21:05:33
   */
  protected Wavelet _wavelet;

  /**
   * Constructor checks whether the given object is all right.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 15.02.2014 21:05:33
   * @param wavelet object of type Wavelet
   */
  protected WaveletTransform(Wavelet wavelet) {
    _wavelet = wavelet;
  } // check for objects od type Wavelet

  /*
   * Returns the stored Wavelet object.
   * @author Christian (graetz23@gmail.com)
   * @date 14.03.2015 18:27:05 (non-Javadoc)
   * @see jwave.transforms.BasicTransform#getWavelet()
   */
  public Wavelet getWavelet() {
    return _wavelet;
  } // getWavelet

  /**
   * Performs a 1-D forward transform from time domain to Hilbert domain using one kind of wavelet
   * transform algorithm for a given array of dimension (length) 2^p | pEN; N = 2, 4, 8, 16, 32, 64,
   * 128, .., and so on.
   *
   * @date 10.02.2010 08:23:24
   * @author Christian (graetz23@gmail.com)
   * @throws JWaveException if given array is not of length 2^p | pEN
   * @see jwave.transforms.BasicTransform#forward(double[])
   */
  @Override
  public double[] forward(double[] arrTime) throws JWaveException {

    if (!isBinary(arrTime.length))
      throw new JWaveFailure(
          "WaveletTransform#forward - "
              + "given array length is not 2^p | p E N ... = 1, 2, 4, 8, 16, 32, .. "
              + "please use the Ancient Egyptian Decomposition for any other array length!");

    int maxLevel = calcExponent(arrTime.length);
    return forward(arrTime, maxLevel); // forward by maximal steps
  } // forward

  /**
   * Performs a 1-D reverse transform from Hilbert domain to time domain using one kind of wavelet
   * transform algorithm for a given array of dimension (length) 2^p | pEN; N = 2, 4, 8, 16, 32, 64,
   * 128, .., and so on.
   *
   * @date 10.02.2010 08:23:24
   * @author Christian (graetz23@gmail.com)
   * @throws JWaveException if given array is not of length 2^p | pEN
   * @see jwave.transforms.BasicTransform#reverse(double[])
   */
  @Override
  public double[] reverse(double[] arrHilb) throws JWaveException {

    if (!isBinary(arrHilb.length))
      throw new JWaveFailure(
          "WaveletTransform#reverse - "
              + "given array length is not 2^p | p E N ... = 1, 2, 4, 8, 16, 32, .. "
              + "please use the Ancient Egyptian Decomposition for any other array length!");

    int maxLevel = calcExponent(arrHilb.length);
    return reverse(arrHilb, maxLevel); // reverse by maximal steps
  } // reverse

  /**
   * Performs several 1-D forward transforms from time domain to all possible Hilbert domains using
   * one kind of wavelet transform algorithm for a given array of dimension (length) 2^p | pEN; N =
   * 2, 4, 8, 16, 32, 64, 128, .., and so on. However, the algorithm stores all levels in a matrix
   * that has in first dimension the range of 0, .., p and in second dimension the coefficients
   * (energy & details) of a certain level. From any level a full reconstruction can be performed.
   * The first dimension is keeping the time series, due to being the Hilbert space of level 0. All
   * following dimensions are keeping the next higher Hilbert spaces, so the next step in wavelet
   * filtering.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 22.03.2015 14:28:49
   * @param arrTime coefficients of time domain
   * @return matDeComp coefficients of frequency or Hilbert domain in 2-D spaces: [ 0 .. p ][ 0 .. M
   *     ] where p is the exponent of M=2^p | pEN
   * @throws JWaveException if something does not match upon the criteria of input
   * @see jwave.transforms.BasicTransform#decompose(double[])
   */
  @Override
  public double[][] decompose(double[] arrTime) throws JWaveException {

    int length = arrTime.length;
    int levels = calcExponent(length);
    double[][] matDeComp = new double[levels + 1][length];
    for (int p = 0; p <= levels; p++)
      System.arraycopy(forward(arrTime, p), 0, matDeComp[p], 0, length);
    return matDeComp;
  } // decompose

  /**
   * Performs one 1-D reverse transform from Hilbert domain to time domain using one kind of wavelet
   * transform algorithm for a given array of dimension (length) 2^p | pEN; N = 2, 4, 8, 16, 32, 64,
   * 128, .., and so on. However, the algorithm uses on of level in a matrix that has in first
   * dimension the range of 0, .., p and in second dimension the coefficients (energy & details) the
   * level. From any level a full a reconstruction can be performed; so from the selected by
   * "level". Anyway, the first dimension is keeping the time series, due to being the Hilbert space
   * of level 0. All following dimensions are keeping the next higher Hilbert spaces, so the next
   * step in wavelet filtering. If one want to denoise each level in the same way and compare
   * results after reverse transform, this is the best input for it.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 22.03.2015 14:29:01
   * @see jwave.transforms.BasicTransform#recompose(double[][], int)
   * @param matDeComp 2-D Hilbert spaces: [ 0 .. p ][ 0 .. M ] where p is the exponent of M=2^p |
   *     pEN
   * @throws JWaveException if something does not match upon the criteria of input
   * @return a 1-D time domain signal
   * @see jwave.transforms.BasicTransform#recompose(double[])
   */
  public double[] recompose(double[][] matDeComp, int level) throws JWaveException {

    if (level < 0 || level >= matDeComp.length)
      throw new JWaveFailure("WaveletTransform#recompose - " + "given level is out of range");

    return reverse(matDeComp[level], level);
  } // recompose
} // WaveletTransform
