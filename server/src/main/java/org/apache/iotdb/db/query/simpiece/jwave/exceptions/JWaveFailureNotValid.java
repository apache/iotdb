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
package org.apache.iotdb.db.query.simpiece.jwave.exceptions;

/**
 * This exception should be thrown if some input or something else is not valid. For example, due to
 * set or known boundaries.
 *
 * @author Christian (graetz23@gmail.com)
 * @date 18.05.2015 20:30:12
 */
public class JWaveFailureNotValid extends JWaveFailure {

  /**
   * @author Christian (graetz23@gmail.com)
   * @date 18.05.2015 20:31:23
   */
  private static final long serialVersionUID = -2046012862738040230L;

  /**
   * @author Christian (graetz23@gmail.com)
   * @date 18.05.2015 20:30:12
   * @param message
   */
  public JWaveFailureNotValid(String message) {
    super(message);
  } // JWaveFailureNotValid
} // class
