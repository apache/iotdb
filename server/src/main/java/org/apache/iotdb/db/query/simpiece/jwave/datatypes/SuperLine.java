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
package org.apache.iotdb.db.query.simpiece.jwave.datatypes;

import org.apache.iotdb.db.query.simpiece.jwave.datatypes.lines.Line;
import org.apache.iotdb.db.query.simpiece.jwave.datatypes.lines.LineFull;
import org.apache.iotdb.db.query.simpiece.jwave.exceptions.JWaveException;
import org.apache.iotdb.db.query.simpiece.jwave.exceptions.JWaveFailureNotValid;
import org.apache.iotdb.db.query.simpiece.jwave.tools.MathToolKit;

import java.util.ArrayList;

/**
 * SuperLine consists of several Line objects of different sizes.
 *
 * @author Christian (graetz23@gmail.com)
 * @date 16.05.2015 19:30:28
 */
public class SuperLine {

  /** maximal size of a line object, */
  private int _maxLineSize;

  /** no of entries in the Line object. */
  private int _noOfRows;

  /**
   * However, who knows how many blocks are delivered!
   *
   * @author Christian (graetz23@gmail.com)
   * @date 18.05.2015 20:02:59
   */
  ArrayList<Super> _listOfLines;

  /**
   * @author Christian (graetz23@gmail.com)
   * @date 16.05.2015 19:30:28
   */
  public SuperLine() {

    _noOfRows = 0;
    _maxLineSize = 1;
    _listOfLines = new ArrayList<Super>();
  } // SuperLine

  /**
   * Set a SuperLine object by the noOfRows or noOfEntries.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 12.01.2016 23:35:12
   * @param noOfRows the number of entries
   */
  public SuperLine(int noOfRows) {

    _noOfRows = noOfRows;
    _maxLineSize = 1;
    _listOfLines = new ArrayList<Super>();
  } // SuperLine

  /**
   * Set the SuperLine object by the noOfRows and the maximal block size.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 12.01.2016 23:36:23
   * @param noOfRows the number of entries
   * @param maxBlockSize the maximal block size, should be << number of entries
   */
  public SuperLine(int noOfRows, int maxBlockSize) {

    _noOfRows = noOfRows;
    _maxLineSize = maxBlockSize;
    _listOfLines = new ArrayList<Super>();
  } // SuperLine

  /**
   * Set up a configured an empty SuperLine object by allocating empty Block objects.
   *
   * @author Christian (graetz23@gmail.com)
   * @throws JWaveException is throwsn while parameters are calcualted wrong
   * @date 12.01.2016 23:38:50
   */
  public void init() throws JWaveException {

    int noOfLineObjects = (int) (_noOfRows / _maxLineSize); // 29 / 8  = 3

    int leftEntries = _noOfRows % _maxLineSize; // 29 % 8 = 5

    if (leftEntries + _maxLineSize * noOfLineObjects != _noOfRows)
      throw new JWaveFailureNotValid("calculated splitting to Block objects is not valid");

    for (int i = 0; i < noOfLineObjects; i++) add(new LineFull(i * _maxLineSize, _maxLineSize));

    int[] arrBinaries = MathToolKit.decompose(leftEntries);
    for (int i = 0; i < arrBinaries.length; i++)
      arrBinaries[i] = (int) MathToolKit.scalb(1., arrBinaries[i]);

    for (int a = 0; a < arrBinaries.length; a++)
      System.out.println("arrbBinaries[ " + a + " ] = " + arrBinaries[a]);

    int offset = noOfLineObjects * _maxLineSize;
    for (int i = 0; i < arrBinaries.length; i++) {
      if (i == 0) add(new LineFull(offset, arrBinaries[i]));
      else {
        offset += arrBinaries[i - 1];
        add(new LineFull(offset, arrBinaries[i]));
      }
    }
  } // init

  /**
   * Returns the number of stored Line objects.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 16.05.2015 19:36:13
   * @return number of Line objects
   */
  public int getNoOfLines() {

    return _listOfLines.size();
  } // getNoOfLines

  /**
   * Add a Line object to list
   *
   * @author Christian (graetz23@gmail.com)
   * @date 16.05.2015 19:36:54
   * @param line object of type Line
   */
  public void add(Line line) {

    _listOfLines.add(line);
  } // add

  /**
   * Return Line object at position p; 0 .. N-1
   *
   * @author Christian (graetz23@gmail.com)
   * @date 24.05.2015 18:16:27
   * @param p
   * @return
   * @throws JWaveException
   */
  public Line get(int p) throws JWaveException {

    Line line = null;

    if (p < 0 || p >= getNoOfLines())
      throw new JWaveFailureNotValid("SuperLine#get - position p is out of bound!");

    line = (Line) _listOfLines.get(p);

    return line;
  } // get

  /**
   * Return the set number of rows
   *
   * @author Christian (graetz23@gmail.com)
   * @date 12.01.2016 23:34:12
   * @return
   */
  public int getNoOfRows() {

    return _noOfRows;
  } // getNoOfRows
} // class
