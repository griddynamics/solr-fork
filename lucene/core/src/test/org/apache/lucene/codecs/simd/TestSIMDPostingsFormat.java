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

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.BasePostingsFormatTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.Ignore;

/**
 * Tests BlockPostingsFormat
 */
public class TestSIMDPostingsFormat extends BasePostingsFormatTestCase {
  private final Codec codec = TestUtil.alwaysPostingsFormat(new SIMDPostingsFormat());

  @Override
  protected Codec getCodec() {
    return codec;
  }

  @Override
  public void testDocsOnly() throws Exception {
    super.testDocsOnly();
  }

  @Override
  @Ignore
  public void testDocsAndFreqs() throws Exception {
    super.testDocsAndFreqs(); // not supported
  }

  @Override
  @Ignore
  public void testDocsAndFreqsAndPositions() throws Exception {
    super.testDocsAndFreqsAndPositions(); // not supported
  }

  @Override
  @Ignore
  public void testDocsAndFreqsAndPositionsAndPayloads() throws Exception {
    super.testDocsAndFreqsAndPositionsAndPayloads();  // not supported
  }

  @Override
  @Ignore
  public void testDocsAndFreqsAndPositionsAndOffsets() throws Exception {
    super.testDocsAndFreqsAndPositionsAndOffsets();  // not supported
  }

  @Override
  @Ignore
  public void testDocsAndFreqsAndPositionsAndOffsetsAndPayloads() throws Exception {
    super.testDocsAndFreqsAndPositionsAndOffsetsAndPayloads();  // not supported
  }

  @Override
  @Ignore
  public void testRandom() throws Exception {
    super.testRandom();  // not supported
  }

  @Override
  @Ignore
  public void testInvertedWrite() throws Exception {
    super.testInvertedWrite();   // not supported
  }

  @Override
  @Ignore
  //todo
  public void testRamBytesUsed() throws IOException {
    super.testRamBytesUsed();
  }
}
