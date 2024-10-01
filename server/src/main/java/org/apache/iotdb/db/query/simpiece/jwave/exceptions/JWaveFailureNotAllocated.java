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
 * This exception should be thrown if some memory is not allocated.
 *
 * @author Christian (graetz23@gmail.com)
 * @date 18.05.2015 20:11:28
 */
public class JWaveFailureNotAllocated extends JWaveFailure {

  /**
   * @author Christian (graetz23@gmail.com)
   * @date 18.05.2015 20:12:36
   */
  private static final long serialVersionUID = 2187309268927118324L;

  /**
   * @author Christian (graetz23@gmail.com)
   * @date 18.05.2015 20:11:28
   * @param message
   */
  public JWaveFailureNotAllocated(String message) {
    super(message);
  } // JWaveFailureNotAllocated
} // class
