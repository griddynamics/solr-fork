package edu;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class Codecs {

  static {
    try {
      System.loadLibrary("util");
    } catch (UnsatisfiedLinkError e) {
      System.err.println("Native code library failed to load from path '" + System.getProperty("java.library.path") + "'" + e);
      System.exit(1);
    }
  }

  /**
   * Compresses data from datain to buffer, returns how many bytes written.
   */
  public native static int compressRegion(int[] datain, byte[] buffer);

  public native static int compressElements(int[] datain, byte[] buffer);

  public native static int compressElementsCritical(int[] datain, byte[] buffer);

  /**
   * Encodes a block of data in {@code pfor} format.
   *
   * @param data    the data to encode
   * @param length  the number of bytes to encode
   * @param buffer a buffer to use to encode data
   * @return how many bytes are written.
   */
  public native static int encodeCritical(int[] data, int offset, int length, byte[] buffer);


  public native static int compressCritical(int[] data, byte[] encoded);

  /**
   * Compresses data from datain to buffer, returns how many bytes written.
   */
  public native static int uncompressRegion(int[] dataout, byte[] buffer);

  public native static int uncompressElements(int[] dataout, byte[] buffer);

  public native static int uncompressElementsCritical(int[] dataout, byte[] buffer);

  /**
   * Compresses data from datain to buffer, returns how many bytes written.
   */
  public native static int uncompressCritical(int[] dataout, byte[] buffer);

}
