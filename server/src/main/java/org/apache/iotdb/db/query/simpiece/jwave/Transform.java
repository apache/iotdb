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
 *
 * @author Christian (graetz23@gmail.com)
 * @date 23.05.2008 17:42:23
 */
package org.apache.iotdb.db.query.simpiece.jwave;

import org.apache.iotdb.db.query.simpiece.jwave.datatypes.natives.Complex;
import org.apache.iotdb.db.query.simpiece.jwave.exceptions.JWaveException;
import org.apache.iotdb.db.query.simpiece.jwave.exceptions.JWaveFailure;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.BasicTransform;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.Wavelet;

/**
 * Base class for transforms like DiscreteFourierTransform, FastBasicTransform, and
 * WaveletPacketTransform.
 *
 * @date 19.05.2009 09:43:40
 * @author Christian (graetz23@gmail.com)
 */
public final class Transform {

  /** Transform object of type base class */
  protected final BasicTransform _basicTransform;

  /**
   * Constructor; needs some object like DiscreteFourierTransform, FastBasicTransform,
   * WaveletPacketTransfom, ...
   *
   * @date 19.05.2009 09:50:24
   * @author Christian (graetz23@gmail.com)
   * @param transform Transform object
   */
  public Transform(BasicTransform transform) {
    _basicTransform = transform;
    try {
      if (_basicTransform == null) throw new JWaveFailure("given object is null!");
      if (!(_basicTransform instanceof BasicTransform))
        throw new JWaveFailure("given object is not of type BasicTransform");
    } catch (JWaveException e) {
      e.showMessage();
      e.printStackTrace();
    } // try
  } // Transform

  /**
   * Performs the forward transform of the specified BasicWave object.
   *
   * @date 10.02.2010 09:41:01
   * @author Christian (graetz23@gmail.com)
   * @param arrTime coefficients of time domain
   * @return coefficients of frequency or Hilbert domain
   */
  public final double[] forward(double[] arrTime) {
    double[] arrHilb = null;
    try {
      arrHilb = _basicTransform.forward(arrTime);
    } catch (JWaveException e) {
      e.showMessage();
      e.printStackTrace();
    } // try
    return arrHilb;
  } // forward

  /**
   * Performs the reverse transform of the specified BasicWave object.
   *
   * @date 10.02.2010 09:42:18
   * @author Christian (graetz23@gmail.com)
   * @param arrHilb coefficients of frequency or Hilbert domain
   * @return coefficients of time domain
   */
  public final double[] reverse(double[] arrHilb) {
    double[] arrTime = null;
    try {
      arrTime = _basicTransform.reverse(arrHilb);
    } catch (JWaveException e) {
      e.showMessage();
      e.printStackTrace();
    } // try
    return arrTime;
  } // reverse

  /**
   * Performs a forward transform to a certain level of Hilbert space.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 22.03.2015 11:53:05
   * @param arrTime array of length 2^p | p E N .. 2, 4, 8, 16, 32, 64, ...
   * @param level a certain level that matches the array
   * @return Hilbert space of certain level
   */
  public final double[] forward(double[] arrTime, int level) {
    double[] arrHilb = null;
    try {
      arrHilb = _basicTransform.forward(arrTime, level);
    } catch (JWaveException e) {
      e.showMessage();
      e.printStackTrace();
    } // try
    return arrHilb;
  } // forward

  /**
   * Performs a reverse transform for a Hilbert space of certain level; level has to match the
   * supported coefficients in the array!
   *
   * @author Christian (graetz23@gmail.com)
   * @date 22.03.2015 11:54:59
   * @param arrHilb Hilbert space by an array of length 2^p | p E N .. 2, 4, 8, 16, 32, 64, ...
   * @param level the certain level the supported hilbert space
   * @return time domain for a certain level of Hilbert space
   */
  public final double[] reverse(double[] arrHilb, int level) {
    double[] arrTime = null;
    try {
      arrTime = _basicTransform.reverse(arrHilb, level);
    } catch (JWaveException e) {
      e.showMessage();
      e.printStackTrace();
    } // try
    return arrTime;
  } // reverse

  /**
   * Performs the forward transform from time domain to frequency or Hilbert domain for a given
   * array depending on the used transform algorithm by inheritance.
   *
   * @date 23.11.2010 19:19:24
   * @author Christian (graetz23@gmail.com)
   * @param arrTime coefficients of 1-D time domain
   * @return coefficients of 1-D frequency or Hilbert domain
   */
  public final Complex[] forward(Complex[] arrTime) {
    Complex[] arrFreq = null;
    try {
      arrFreq = ((BasicTransform) _basicTransform).forward(arrTime);
    } catch (JWaveException e) {
      e.showMessage();
      e.printStackTrace();
    } // try
    return arrFreq;
  } // forward

  /**
   * Performs the reverse transform from frequency or Hilbert domain to time domain for a given
   * array depending on the used transform algorithm by inheritance.
   *
   * @date 23.11.2010 19:19:33
   * @author Christian (graetz23@gmail.com)
   * @param arrFreq coefficients of 1-D frequency or Hilbert domain
   * @return coefficients of 1-D time domain
   */
  public final Complex[] reverse(Complex[] arrFreq) {
    Complex[] arrTime = null;
    try {
      arrTime = ((BasicTransform) _basicTransform).reverse(arrFreq);
    } catch (JWaveException e) {
      e.showMessage();
      e.printStackTrace();
    } // try
    return arrTime;
  } // reverse

  /**
   * Performs the 2-D forward transform of the specified BasicWave object.
   *
   * @date 10.02.2010 10:58:54
   * @author Christian (graetz23@gmail.com)
   * @param matrixTime coefficients of 2-D time domain; internal M(i),N(j)
   * @return coefficients of 2-D frequency or Hilbert domain
   */
  public final double[][] forward(double[][] matrixTime) {
    double[][] matrixHilb = null;
    try {
      matrixHilb = _basicTransform.forward(matrixTime);
    } catch (JWaveException e) {
      e.showMessage();
      e.printStackTrace();
    } // try
    return matrixHilb;
  } // forward

  /**
   * Performs the 2-D reverse transform of the specified BasicWave object.
   *
   * @date 10.02.2010 10:59:32
   * @author Christian (graetz23@gmail.com)
   * @param matrixFreq coefficients of 2-D frequency or Hilbert domain; internal M(i),N(j)
   * @return coefficients of 2-D time domain
   */
  public final double[][] reverse(double[][] matrixHilb) {
    double[][] matrixTime = null;
    try {
      matrixTime = _basicTransform.reverse(matrixHilb);
    } catch (JWaveException e) {
      e.showMessage();
      e.printStackTrace();
    } // try
    return matrixTime;
  } // reverse

  /**
   * Performs the 2-D forward transform of the specified BasicWave object.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 22.03.2015 14:40:54
   * @param matrixTime coefficients of 2-D time domain; internal M(i),N(j)
   * @param levelM a certain level to stop transform for over rows
   * @param levelN a certain level to stop transform for over columns
   * @return coefficients of 2-D frequency or Hilbert domain
   */
  public final double[][] forward(double[][] matrixTime, int levelM, int levelN) {
    double[][] matrixHilb = null;
    try {
      matrixHilb = _basicTransform.forward(matrixTime, levelM, levelN);
    } catch (JWaveException e) {
      e.showMessage();
      e.printStackTrace();
    } // try
    return matrixHilb;
  } // forward

  /**
   * Performs the 2-D reverse transform of the specified BasicWave object.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 22.03.2015 14:42:12
   * @param matrixFreq coefficients of 2-D frequency or Hilbert domain; internal M(i),N(j)
   * @param levelM a certain level to stop transform for over rows
   * @param levelN a certain level to stop transform for over columns
   * @return coefficients of 2-D time domain
   */
  public final double[][] reverse(double[][] matrixHilb, int levelM, int levelN) {
    double[][] matrixTime = null;
    try {
      matrixTime = _basicTransform.reverse(matrixHilb, levelM, levelN);
    } catch (JWaveException e) {
      e.showMessage();
      e.printStackTrace();
    } // try
    return matrixTime;
  } // reverse

  /**
   * Performs the 3-D forward transform of the specified BasicWave object.
   *
   * @date 10.07.2010 18:15:22
   * @author Christian (graetz23@gmail.com)
   * @param matrixTime coefficients of 2-D time domain; internal M(i),N(j),O(k)
   * @return coefficients of 2-D frequency or Hilbert domain
   */
  public final double[][][] forward(double[][][] spaceTime) {
    double[][][] spaceHilb = null;
    try {
      spaceHilb = _basicTransform.forward(spaceTime);
    } catch (JWaveException e) {
      e.showMessage();
      e.printStackTrace();
    } // try
    return spaceHilb;
  } // forward

  /**
   * Performs the 3-D reverse transform of the specified BasicWave object.
   *
   * @date 10.07.2010 18:15:33
   * @author Christian (graetz23@gmail.com)
   * @param matrixFreq coefficients of 2-D frequency or Hilbert domain; internal M(i),N(j),O(k)
   * @return coefficients of 2-D time domain
   */
  public final double[][][] reverse(double[][][] spaceHilb) {
    double[][][] spaceTime = null;
    try {
      spaceTime = _basicTransform.reverse(spaceHilb);
    } catch (JWaveException e) {
      e.showMessage();
      e.printStackTrace();
    } // try
    return spaceTime;
  } // reverse

  /**
   * Performs the 3-D forward transform of the specified BasicWave object.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 22.03.2015 14:45:46
   * @param spaceTime coefficients of 2-D time domain; internal M(i),N(j),O(k)
   * @param levelP a certain level to stop transform for over rows
   * @param levelQ a certain level to stop transform for over columns
   * @param levelR a certain level to stop transform for over height
   * @return coefficients of 2-D frequency or Hilbert domain
   */
  public final double[][][] forward(double[][][] spaceTime, int levelP, int levelQ, int levelR) {
    double[][][] spaceHilb = null;
    try {
      spaceHilb = _basicTransform.forward(spaceTime, levelP, levelQ, levelR);
    } catch (JWaveException e) {
      e.showMessage();
      e.printStackTrace();
    } // try
    return spaceHilb;
  } // forward

  /**
   * Performs the 3-D reverse transform of the specified BasicWave object.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 22.03.2015 14:46:09
   * @param spaceHilb coefficients of 2-D frequency or Hilbert domain; internal M(i),N(j),O(k)
   * @param levelP a certain level to start transform from over rows
   * @param levelQ a certain level to stop transform for over columns
   * @param levelR a certain level to start transform from over height
   * @return coefficients of 2-D time domain
   */
  public final double[][][] reverse(double[][][] spaceHilb, int levelP, int levelQ, int levelR) {
    double[][][] spaceTime = null;
    try {
      spaceTime = _basicTransform.reverse(spaceHilb, levelP, levelQ, levelR);
    } catch (JWaveException e) {
      e.showMessage();
      e.printStackTrace();
    } // try
    return spaceTime;
  } // reverse

  /**
   * Generates from a 1D signal a 2D output, where the second dimension are the levels of the
   * wavelet transform.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 17.08.2014 10:07:19
   * @param arrTime coefficients of time domain
   * @return matDeComp 2-D Hilbert spaces: [ 0 .. p ][ 0 .. N ] where p is the exponent of N=2^p
   */
  public final double[][] decompose(double[] arrTime) {
    double[][] matDeComp = null;
    try {
      matDeComp = _basicTransform.decompose(arrTime);
    } catch (JWaveException e) {
      e.showMessage();
      e.printStackTrace();
    } // try
    return matDeComp;
  } // decompose

  /**
   * Generates from a 2-D decomposition a 1-D time series.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 17.08.2014 10:07:19
   * @param matDeComp 2-D Hilbert spaces: [ 0 .. p ][ 0 .. N ] where p is the exponent of N=2^p
   * @return a 1-D time domain signal
   */
  public final double[] recompose(double[][] matDeComp) {
    double[] arrTime = null;
    try {
      arrTime = _basicTransform.recompose(matDeComp);
    } catch (JWaveException e) {
      e.showMessage();
      e.printStackTrace();
    } // try
    return arrTime;
  } // recompose

  /**
   * Recompose signal from a certain level of decomposition.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 22.03.2015 10:08:52
   * @param matDeComp
   * @param level
   * @return
   */
  public final double[] recompose(double[][] matDeComp, int level) {
    double[] arrTime = null;
    try {
      arrTime = _basicTransform.recompose(matDeComp, level);
    } catch (JWaveException e) {
      e.showMessage();
      e.printStackTrace();
    } // try
    return arrTime;
  } // recompose

  /**
   * Return the used object of type BasicTransform.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 14.03.2015 18:19:13
   * @return identifier of object of type Basic Transform
   */
  public final BasicTransform getBasicTransform() {

    BasicTransform basicTransform = null;

    try {

      if (_basicTransform == null)
        throw new JWaveFailure("Transform - BasicTransform object is null!");

      if (!(_basicTransform instanceof BasicTransform))
        throw new JWaveFailure(
            "Transform - getBasicTransform - " + "member is not of type BasicTransform!");

      basicTransform = _basicTransform;

    } catch (JWaveException e) {
      e.showMessage();
      e.printStackTrace();
    } // try

    return basicTransform;
  } // getBasicTransform

  /**
   * Returns the used Wavelet object or null pointer.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 14.03.2015 18:58:50
   * @return object of type Wavelet
   * @throws JWaveFailure if Wavelet object is not available
   */
  public final Wavelet getWavelet() {

    Wavelet wavelet = null;
    BasicTransform basicTransform = null;

    try {

      basicTransform = getBasicTransform();
      wavelet = basicTransform.getWavelet();

    } catch (JWaveException e) {
      e.showMessage();
      e.printStackTrace();
    } // try

    return wavelet;
  } // getWavelet
} // class
