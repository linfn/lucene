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
package org.apache.lucene.document;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestSortedNumericDocValuesSetSkippingIterator extends LuceneTestCase {

  public void testSingleLevel() throws IOException {
    doTestBasics(false);
  }

  public void testMultipleLevels() throws IOException {
    doTestBasics(true);
  }

  public void testAdvanceReturnsNoMoreDocsWhenNoIntervalsIntersect() throws IOException {
    AtomicBoolean twoPhaseCalled = new AtomicBoolean();
    NumericDocValues values = newNumericDocValues(0, 20);
    TwoPhaseIterator twoPhase =
        new TwoPhaseIterator(values) {
          @Override
          public boolean matches() throws IOException {
            twoPhaseCalled.set(true);
            return true;
          }

          @Override
          public float matchCost() {
            return 3f;
          }
        };
    DocValuesSkipper skipper =
        newSingleLevelSkipper(
            new int[] {0, 20}, new int[] {9, 29}, new long[] {10, 10}, new long[] {10, 10}, new int[] {1, 1});

    SortedNumericDocValuesSetSkippingIterator setIterator =
        new SortedNumericDocValuesSetSkippingIterator(
            twoPhase, skipper, new DocValuesLongHashSet(new long[] {11}));
    SortedNumericDocValuesSetSkippingIterator.Approximation setApproximation =
        (SortedNumericDocValuesSetSkippingIterator.Approximation) setIterator.approximation();

    assertEquals(DocIdSetIterator.NO_MORE_DOCS, setApproximation.advance(0));
    assertFalse(twoPhaseCalled.get());
  }

  public void testAdvanceWhenTargetIsBetweenBlocks() throws IOException {
    AtomicBoolean twoPhaseCalled = new AtomicBoolean();
    NumericDocValues values =
        newNumericDocValues(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29);
    TwoPhaseIterator twoPhase =
        new TwoPhaseIterator(values) {
          @Override
          public boolean matches() throws IOException {
            twoPhaseCalled.set(true);
            return values.longValue() == 10;
          }

          @Override
          public float matchCost() {
            return 3f;
          }
        };
    DocValuesSkipper skipper =
        newSingleLevelSkipper(
            new int[] {0, 20}, new int[] {9, 29}, new long[] {10, 10}, new long[] {10, 10}, new int[] {10, 10});

    SortedNumericDocValuesSetSkippingIterator setIterator =
        new SortedNumericDocValuesSetSkippingIterator(
            twoPhase, skipper, new DocValuesLongHashSet(new long[] {10}));
    SortedNumericDocValuesSetSkippingIterator.Approximation setApproximation =
        (SortedNumericDocValuesSetSkippingIterator.Approximation) setIterator.approximation();

    assertEquals(20, setApproximation.advance(15));
    assertEquals(SortedNumericDocValuesSetSkippingIterator.Match.YES, setApproximation.match);
    assertEquals(29, setApproximation.upTo);
    assertTrue(setIterator.matches());
    assertTrue(values.docID() < setApproximation.docID());
    assertFalse(twoPhaseCalled.get());
  }

  public void testAdvancePastCurrentBlockWhenApproximationOversteps() throws IOException {
    AtomicBoolean twoPhaseCalled = new AtomicBoolean();
    NumericDocValues values = newNumericDocValues(0, 20);
    TwoPhaseIterator twoPhase =
        new TwoPhaseIterator(values) {
          @Override
          public boolean matches() throws IOException {
            twoPhaseCalled.set(true);
            return values.longValue() == 10;
          }

          @Override
          public float matchCost() {
            return 3f;
          }
        };
    DocValuesSkipper skipper =
        newSingleLevelSkipper(
            new int[] {0, 20}, new int[] {9, 29}, new long[] {10, 10}, new long[] {10, 10}, new int[] {1, 1});

    SortedNumericDocValuesSetSkippingIterator setIterator =
        new SortedNumericDocValuesSetSkippingIterator(
            twoPhase, skipper, new DocValuesLongHashSet(new long[] {10}));
    SortedNumericDocValuesSetSkippingIterator.Approximation setApproximation =
        (SortedNumericDocValuesSetSkippingIterator.Approximation) setIterator.approximation();

    assertEquals(20, setApproximation.advance(5));
    assertEquals(
        SortedNumericDocValuesSetSkippingIterator.Match.IF_DOC_HAS_VALUE, setApproximation.match);
    assertEquals(29, setApproximation.upTo);
    assertEquals(values.docID(), setApproximation.docID());
    assertTrue(setIterator.matches());
    assertFalse(twoPhaseCalled.get());
  }

  private void doTestBasics(boolean doLevels) throws IOException {
    NumericDocValues values =
        new NumericDocValues() {

          int doc = -1;

          @Override
          public boolean advanceExact(int target) throws IOException {
            throw new UnsupportedOperationException();
          }

          @Override
          public int docID() {
            return doc;
          }

          @Override
          public int nextDoc() throws IOException {
            return advance(doc + 1);
          }

          @Override
          public int advance(int target) throws IOException {
            if (target < 1536) {
              // dense up to 1536
              return doc = target;
            } else if (doc < 2047) {
              // only even docs have a value from 1536 to 2048
              return doc = target + (target & 1);
            } else {
              return doc = DocIdSetIterator.NO_MORE_DOCS;
            }
          }

          @Override
          public long longValue() throws IOException {
            if (doc < 512) {
              return 10;
            } else if (doc < 1024) {
              return 100;
            } else if (doc < 1536) {
              return switch (doc % 3) {
                case 0 -> 9;
                case 1 -> 30;
                case 2 -> 10;
                default -> throw new AssertionError();
              };
            } else {
              return 10;
            }
          }

          @Override
          public long cost() {
            return 42;
          }
        };

    AtomicBoolean twoPhaseCalled = new AtomicBoolean();
    TwoPhaseIterator twoPhase =
        new TwoPhaseIterator(values) {

          @Override
          public boolean matches() throws IOException {
            twoPhaseCalled.set(true);
            return values.longValue() == 10;
          }

          @Override
          public float matchCost() {
            return 3f;
          }
        };

    DocValuesSkipper skipper =
        new DocValuesSkipper() {

          int doc = -1;

          @Override
          public void advance(int target) throws IOException {
            doc = target;
          }

          @Override
          public int numLevels() {
            return doLevels ? 2 : 1;
          }

          @Override
          public int minDocID(int level) {
            int rangeLog = 8 + level;
            if (doc < 0) {
              return -1;
            } else if (doc >= 2048) {
              return DocIdSetIterator.NO_MORE_DOCS;
            } else {
              int mask = (1 << rangeLog) - 1;
              return doc & ~mask;
            }
          }

          @Override
          public int maxDocID(int level) {
            int rangeLog = 8 + level;
            int minDocID = minDocID(level);
            return switch (minDocID) {
              case -1 -> -1;
              case DocIdSetIterator.NO_MORE_DOCS -> DocIdSetIterator.NO_MORE_DOCS;
              default -> minDocID + (1 << rangeLog) - 1;
            };
          }

          @Override
          public long minValue(int level) {
            int minDocID = minDocID(level);
            if (minDocID < 512) {
              return 10;
            } else if (minDocID < 1024) {
              return 100;
            } else if (minDocID < 1536) {
              return 9;
            } else {
              return 10;
            }
          }

          @Override
          public long maxValue(int level) {
            int minDocID = minDocID(level);
            if (minDocID < 512) {
              return 10;
            } else if (minDocID < 1024) {
              return 100;
            } else if (minDocID < 1536) {
              return 30;
            } else {
              return 10;
            }
          }

          @Override
          public int docCount(int level) {
            int rangeLog = 8 + level;
            if (minDocID(level) < 1536) {
              return 1 << rangeLog;
            } else {
              return 1 << (rangeLog - 1);
            }
          }

          @Override
          public long minValue() {
            return 9;
          }

          @Override
          public long maxValue() {
            return 100;
          }

          @Override
          public int docCount() {
            return 1536 + 256;
          }
        };

    SortedNumericDocValuesSetSkippingIterator setIterator =
        new SortedNumericDocValuesSetSkippingIterator(
            twoPhase, skipper, new DocValuesLongHashSet(new long[] {10}));
    SortedNumericDocValuesSetSkippingIterator.Approximation setApproximation =
        (SortedNumericDocValuesSetSkippingIterator.Approximation) setIterator.approximation();

    assertEquals(100, setApproximation.advance(100));
    assertEquals(SortedNumericDocValuesSetSkippingIterator.Match.YES, setApproximation.match);
    assertEquals(doLevels ? 511 : 255, setApproximation.upTo);
    assertTrue(setIterator.matches());
    assertTrue(values.docID() < setApproximation.docID()); // we did not advance doc values
    assertFalse(twoPhaseCalled.get());

    assertEquals(1024, setApproximation.advance(600));
    assertEquals(SortedNumericDocValuesSetSkippingIterator.Match.MAYBE, setApproximation.match);
    assertEquals(1279, setApproximation.upTo);
    for (int i = 0; i < 10; ++i) {
      assertEquals(values.docID(), setApproximation.docID());
      assertEquals(twoPhase.matches(), setIterator.matches());
      assertTrue(twoPhaseCalled.get());
      twoPhaseCalled.set(false);
      setApproximation.nextDoc();
    }

    assertEquals(1600, setApproximation.advance(1600));
    assertEquals(
        SortedNumericDocValuesSetSkippingIterator.Match.IF_DOC_HAS_VALUE, setApproximation.match);
    assertEquals(doLevels ? 2047 : 1791, setApproximation.upTo);
    assertEquals(values.docID(), setApproximation.docID());
    assertTrue(setIterator.matches());
    assertFalse(twoPhaseCalled.get());

    assertEquals(DocIdSetIterator.NO_MORE_DOCS, setApproximation.advance(2048));
  }

  private NumericDocValues newNumericDocValues(int... docsWithValue) {
    return new NumericDocValues() {

      int index = -1;
      int doc = -1;

      @Override
      public boolean advanceExact(int target) throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public int docID() {
        return doc;
      }

      @Override
      public int nextDoc() throws IOException {
        return advance(doc + 1);
      }

      @Override
      public int advance(int target) throws IOException {
        int nextIndex = index + 1;
        while (nextIndex < docsWithValue.length && docsWithValue[nextIndex] < target) {
          nextIndex++;
        }
        index = nextIndex;
        if (index == docsWithValue.length) {
          return doc = DocIdSetIterator.NO_MORE_DOCS;
        }
        return doc = docsWithValue[index];
      }

      @Override
      public long longValue() throws IOException {
        return 10;
      }

      @Override
      public long cost() {
        return docsWithValue.length;
      }
    };
  }

  private DocValuesSkipper newSingleLevelSkipper(
      int[] minDocIds, int[] maxDocIds, long[] minValues, long[] maxValues, int[] docCounts) {
    return new DocValuesSkipper() {

      int block = -1;

      @Override
      public void advance(int target) throws IOException {
        int nextBlock = block + 1;
        while (nextBlock < maxDocIds.length && maxDocIds[nextBlock] < target) {
          nextBlock++;
        }
        block = nextBlock;
      }

      @Override
      public int numLevels() {
        return 1;
      }

      @Override
      public int minDocID(int level) {
        if (block < 0) {
          return -1;
        } else if (block >= minDocIds.length) {
          return DocIdSetIterator.NO_MORE_DOCS;
        }
        return minDocIds[block];
      }

      @Override
      public int maxDocID(int level) {
        if (block < 0) {
          return -1;
        } else if (block >= maxDocIds.length) {
          return DocIdSetIterator.NO_MORE_DOCS;
        }
        return maxDocIds[block];
      }

      @Override
      public long minValue(int level) {
        return minValues[block];
      }

      @Override
      public long maxValue(int level) {
        return maxValues[block];
      }

      @Override
      public int docCount(int level) {
        return docCounts[block];
      }

      @Override
      public long minValue() {
        return minValues[0];
      }

      @Override
      public long maxValue() {
        return maxValues[maxValues.length - 1];
      }

      @Override
      public int docCount() {
        int count = 0;
        for (int docCount : docCounts) {
          count += docCount;
        }
        return count;
      }
    };
  }
}
