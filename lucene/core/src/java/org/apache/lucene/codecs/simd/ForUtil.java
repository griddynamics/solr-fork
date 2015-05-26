package org.apache.lucene.codecs.simd;
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

import java.io.IOException;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.packed.PackedInts;

import static org.apache.lucene.codecs.simd.SIMDPostingsFormat.BLOCK_SIZE;

/**
 * Encode all values in normal area with fixed bit width,
 * which is determined by the max value in this block.
 */
final class ForUtil {

  /**
   * Upper limit of the number of bytes that might be required to stored
   * <code>BLOCK_SIZE</code> encoded values.
   */
  static final int MAX_ENCODED_SIZE = BLOCK_SIZE * 8;

  /**
   * Upper limit of the number of values that might be decoded in a single call to
   * {@link #readBlock(org.apache.lucene.store.IndexInput, byte[], int[])}. Although values after
   * <code>BLOCK_SIZE</code> are garbage, it is necessary to allocate value buffers
   * whose size is >= MAX_DATA_SIZE to avoid {@link ArrayIndexOutOfBoundsException}s.
   */
  static final int MAX_DATA_SIZE = 1024;

  private static final byte ALL_VALUES_EQUAL = 0;

  /**
   * Create a new {@link org.apache.lucene.codecs.lucene50.ForUtil} instance and save state into <code>out</code>.
   */
  ForUtil(DataOutput out) throws IOException {
    out.writeVInt(PackedInts.VERSION_CURRENT);
  }

  /**
   * Restore a {@link org.apache.lucene.codecs.lucene50.ForUtil} from a {@link org.apache.lucene.store.DataInput}.
   */
  ForUtil(DataInput in) throws IOException {
    int packedIntsVersion = in.readVInt();
    PackedInts.checkVersion(packedIntsVersion);
  }

  /**
   * Write a block of data (<code>For</code> format).
   *
   * @param data    the data to write
   * @param encoded a buffer to use to encode data
   * @param out     the destination output
   * @throws java.io.IOException If there is a low-level I/O error
   */
  void writeBlock(int[] data, byte[] encoded, IndexOutput out) throws IOException {
    if (isAllEqual(data)) {
      out.writeByte(ALL_VALUES_EQUAL);
      out.writeVInt(data[0]);
      out.writeVInt(data[1] - data[0]);
      return;
    }

    int encodedSize = edu.Codecs.encodeCritical(data, 0, BLOCK_SIZE, encoded);
    out.writeBytes(encoded, encodedSize);
  }

  private static boolean isAllEqual(final int[] data) {
    final int v = data[1] - data[0];
    for (int i = 2; i < BLOCK_SIZE; i++) {
      if (data[i] - data[i - 1] != v) {
        return false;
      }
    }
    return true;
  }

  /**
   * Read the next block of data (<code>For</code> format).
   *
   * @param in      the input to use to read data
   * @param encoded a buffer that can be used to store encoded data
   * @param decoded where to write decoded data
   * @throws java.io.IOException If there is a low-level I/O error
   */
  void readBlock(IndexInput in, byte[] encoded, int[] decoded) throws IOException {
    final byte numBits = in.readByte();
    assert numBits <= 32 : numBits;
    if (numBits == ALL_VALUES_EQUAL) {
      final int value = in.readVInt();
      int delta = in.readVInt();
      for (int i = 0; i < BLOCK_SIZE; i++) {
        decoded[i] = value + i * delta;
      }
      return;
    }

    encoded[0] = numBits;
    in.readBytes(encoded, 1, numBits << 4);
    edu.Codecs.decodeCritical(encoded, decoded);
  }

  /**
   * Skip the next block of data.
   *
   * @param in the input where to read data
   * @throws java.io.IOException If there is a low-level I/O error
   */
  void skipBlock(IndexInput in) throws IOException {
    final int numBits = in.readByte();
    if (numBits == ALL_VALUES_EQUAL) {
      in.readVInt();
      in.readVInt();
      return;
    }
    assert numBits > 0 && numBits <= 32 : numBits;
    in.seek(in.getFilePointer() + (numBits << 4));
  }
}
