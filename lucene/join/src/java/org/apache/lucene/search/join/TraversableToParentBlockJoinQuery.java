package org.apache.lucene.search.join;

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
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.Set;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ComplexExplanation;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
// javadocs

/**
 * This query requires that you index
 * children and parent docs as a single block, using the
 * {@link IndexWriter#addDocuments IndexWriter.addDocuments()} or {@link
 * IndexWriter#updateDocuments IndexWriter.updateDocuments()} API.  In each block, the
 * child documents must appear first, ending with the parent
 * document.  At search time you provide a Filter
 * identifying the parents, however this Filter must provide
 * an {@link FixedBitSet} per sub-reader.
 *
 * <p>Once the block index is built, use this query to wrap
 * any sub-query matching only child docs and join matches in that
 * child document space up to the parent document space.
 * You can then use this Query as a clause with
 * other queries in the parent document space.</p>
 *
 * <p>See {@link ToChildBlockJoinQuery} if you need to join
 * in the reverse order.
 *
 * <p>The child documents must be orthogonal to the parent
 * documents: the wrapped child query must never
 * return a parent document.</p>
 *
 * If you'd like to retrieve {@link TopGroups} for the
 * resulting query, use the {@link ToParentBlockJoinCollector}.
 * Note that this is not necessary, ie, if you simply want
 * to collect the parent documents and don't need to see
 * which child documents matched under that parent, then
 * you can use any collector.
 *
 * <p><b>NOTE</b>: If the overall query contains parent-only
 * matches, for example you OR a parent-only query with a
 * joined child-only query, then the resulting collected documents
 * will be correct, however the {@link TopGroups} you get
 * from {@link ToParentBlockJoinCollector} will not contain every
 * child for parents that had matched.
 *
 * <p>See {@link org.apache.lucene.search.join} for an
 * overview. </p>
 *
 * @lucene.experimental
 */
public class TraversableToParentBlockJoinQuery extends Query {

    protected final Filter parentsFilter;
    protected final Query childQuery;

    // If we are rewritten, this is the original childQuery we
    // were passed; we use this for .equals() and
    // .hashCode().  This makes rewritten query equal the
    // original, so that user does not have to .rewrite() their
    // query before searching:
    protected final Query origChildQuery;

    /** Create an TraversableToParentBlockJoinQuery.
     *
     * @param childQuery Query matching child documents.
     * @param parentsFilter Filter (must produce FixedBitSet
     * per-segment) identifying the parent documents.
    **/
    public TraversableToParentBlockJoinQuery(Query childQuery, Filter parentsFilter) {
        super();
        this.origChildQuery = childQuery;
        this.childQuery = childQuery;
        this.parentsFilter = parentsFilter;
    }

    protected TraversableToParentBlockJoinQuery(Query origChildQuery, Query childQuery, Filter parentsFilter) {
        super();
        this.origChildQuery = origChildQuery;
        this.childQuery = childQuery;
        this.parentsFilter = parentsFilter;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher) throws IOException {
        return new TraversableBlockJoinWeight(this, childQuery.createWeight(searcher), parentsFilter);
    }

    protected static class TraversableBlockJoinWeight extends Weight {
        protected final Query joinQuery;
        protected final Weight childWeight;
        protected final Filter parentsFilter;

        public TraversableBlockJoinWeight(Query joinQuery, Weight childWeight, Filter parentsFilter) {
            super();
            this.joinQuery = joinQuery;
            this.childWeight = childWeight;
            this.parentsFilter = parentsFilter;
        }

        // NOTE: acceptDocs applies (and is checked) only in the
        // parent document space
        @Override
        public Scorer scorer(AtomicReaderContext readerContext, boolean scoreDocsInOrder,
            boolean topScorer, Bits acceptDocs) throws IOException {

            // Pass scoreDocsInOrder true, topScorer false to our sub:
            final Scorer childScorer = childWeight.scorer(readerContext, true, false, null);

            if (childScorer == null) {
                // No matches
                return null;
            }

            final int firstChildDoc = childScorer.nextDoc();
            if (firstChildDoc == DocIdSetIterator.NO_MORE_DOCS) {
                // No matches
                return null;
            }

            // NOTE: we cannot pass acceptDocs here because this
            // will (most likely, justifiably) cause the filter to
            // not return a FixedBitSet but rather a
            // BitsFilteredDocIdSet.  Instead, we filter by
            // acceptDocs when we score:
            final DocIdSet parents = parentsFilter.getDocIdSet(readerContext, null);

            if (parents == null
                || parents.iterator().docID() == DocIdSetIterator.NO_MORE_DOCS) { // <-- means DocIdSet#EMPTY_DOCIDSET
                // No matches
                return null;
            }
            if (!(parents instanceof FixedBitSet)) {
                throw new IllegalStateException("parentFilter must return FixedBitSet; got " + parents);
            }

            return new TraversableBlockJoinScorer(this, childScorer, (FixedBitSet) parents, firstChildDoc, acceptDocs);
        }

        @Override
        public Query getQuery() {
            return joinQuery;
        }

        @Override
        public float getValueForNormalization() throws IOException {
            return childWeight.getValueForNormalization() * joinQuery.getBoost() * joinQuery.getBoost();
        }

        @Override
        public void normalize(float norm, float topLevelBoost) {
            childWeight.normalize(norm, topLevelBoost * joinQuery.getBoost());
        }

        @Override
        public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
            TraversableBlockJoinScorer scorer = (TraversableBlockJoinScorer) scorer(context, true, false, context.reader().getLiveDocs());
            if (scorer != null) {
                if (scorer.advance(doc) == doc) {
                    return scorer.explain(context.docBase);
                }
            }
            return new ComplexExplanation(false, 0.0f, "Not a match");
        }

        @Override
        public boolean scoresDocsOutOfOrder() {
            return false;
        }
    }

    public static class TraversableBlockJoinScorer extends Scorer {

        public static final int NO_MORE_CHILDREN = Integer.MAX_VALUE;

        protected final Scorer childScorer;
        protected final FixedBitSet parentBits;
        protected final Bits acceptDocs;
        protected int parentDoc = -1;
        protected int prevParentDoc;
        protected float parentScore;
        protected int parentFreq;
        protected int nextChildDoc;

        public TraversableBlockJoinScorer(Weight weight, Scorer childScorer, FixedBitSet parentBits, int firstChildDoc, Bits acceptDocs) {
            super(weight);
            //System.out.println("Q.init firstChildDoc=" + firstChildDoc);
            this.parentBits = parentBits;
            this.childScorer = childScorer;
            this.acceptDocs = acceptDocs;
            nextChildDoc = firstChildDoc;
        }

        @Override
        public Collection<ChildScorer> getChildren() {
            return Collections.singleton(new ChildScorer(childScorer, "BLOCK_JOIN"));
        }

        @Override
        public int nextDoc() throws IOException {
            //System.out.println("Q.nextDoc() nextChildDoc=" + nextChildDoc);

            // Loop until we hit a parentDoc that's accepted
            while (true) {
                // Advance child scorer to the next parent's first child if it wasn't done by nextChild()
                if (nextChildDoc < parentDoc && parentDoc != NO_MORE_DOCS) {
                    nextChildDoc = childScorer.advance(parentDoc + 1);
                }

                if (nextChildDoc == NO_MORE_DOCS) {
                    //System.out.println("  end");
                    return parentDoc = NO_MORE_DOCS;
                }

                // Gather all children sharing the same parent as
                // nextChildDoc

                parentDoc = parentBits.nextSetBit(nextChildDoc);

                //System.out.println("  parentDoc=" + parentDoc);
                assert parentDoc != -1;

                //System.out.println("  nextChildDoc=" + nextChildDoc);
                if (acceptDocs != null && !acceptDocs.get(parentDoc)) {
                    // Parent doc not accepted; skip child docs until
                    // we hit a new parent doc:
                    while(nextChild() != NO_MORE_CHILDREN);
                    continue;
                }

                // Parent & child docs are supposed to be orthogonal:
                assert nextChildDoc != parentDoc;

                //System.out.println("  return parentDoc=" + parentDoc);
                return parentDoc;
            }
        }

        public int nextChild() throws IOException {
            nextChildDoc = childScorer.nextDoc();
            return nextChildDoc < parentDoc ? nextChildDoc : NO_MORE_CHILDREN;
        }

        @Override
        public int docID() {
            return parentDoc;
        }

        @Override
        public float score() throws IOException {
            childScorer.score(); // workaround to push all boolean scorers in case of RecOptSum child scorer
            return parentScore;
        }

        @Override
        public int freq() {
            return parentFreq;
        }

        @Override
        public int advance(int parentTarget) throws IOException {

            //System.out.println("Q.advance parentTarget=" + parentTarget);
            if (parentTarget == NO_MORE_DOCS) {
                return parentDoc = NO_MORE_DOCS;
            }

            if (parentTarget == 0) {
                // Callers should only be passing in a docID from
                // the parent space, so this means this parent
                // has no children (it got docID 0), so it cannot
                // possibly match.  We must handle this case
                // separately otherwise we pass invalid -1 to
                // prevSetBit below:
                return nextDoc();
            }

            prevParentDoc = parentBits.prevSetBit(parentTarget-1);

            //System.out.println("  rolled back to prevParentDoc=" + prevParentDoc + " vs parentDoc=" + parentDoc);
            assert prevParentDoc >= parentDoc;
            if (prevParentDoc > nextChildDoc) {
                nextChildDoc = childScorer.advance(prevParentDoc);
            // System.out.println("  childScorer advanced to child docID=" + nextChildDoc);
            //} else {
                //System.out.println("  skip childScorer advance");
            }

            // Parent & child docs are supposed to be orthogonal:
            assert nextChildDoc != prevParentDoc;

            final int nd = nextDoc();
            //System.out.println("  return nextParentDoc=" + nd);
            return nd;
        }

        public Explanation explain(int docBase) throws IOException {
            int start = docBase + prevParentDoc + 1; // +1 b/c prevParentDoc is previous parent doc
            int end = docBase + parentDoc - 1; // -1 b/c parentDoc is parent doc
            return new ComplexExplanation(
                  true, score(), String.format(Locale.ROOT, "Score based on child doc range from %d to %d", start, end)
            );
        }
    }

    @Override
    public void extractTerms(Set<Term> terms) {
        childQuery.extractTerms(terms);
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        final Query childRewrite = childQuery.rewrite(reader);
        if (childRewrite != childQuery) {
            Query rewritten = new TraversableToParentBlockJoinQuery(childQuery,
                                                                 childRewrite,
                                                                 parentsFilter);
            rewritten.setBoost(getBoost());
            return rewritten;
        } else {
            return this;
        }
    }

    @Override
    public String toString(String field) {
        return "TraversableToParentBlockJoinQuery ("+childQuery.toString()+")";
    }

    @Override
    public TraversableToParentBlockJoinQuery clone() {
        return new TraversableToParentBlockJoinQuery(origChildQuery.clone(),
                                                  parentsFilter);
    }
    
    public Query getChildQuery() {
      return childQuery;
    }

    @Override
    public boolean equals(Object _other) {
        if (_other instanceof TraversableToParentBlockJoinQuery) {
            final TraversableToParentBlockJoinQuery other = (TraversableToParentBlockJoinQuery) _other;
            return origChildQuery.equals(other.origChildQuery) &&
                   parentsFilter.equals(other.parentsFilter);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int hash = 1;
        hash = prime * hash + origChildQuery.hashCode();
        hash = prime * hash + parentsFilter.hashCode();
        return hash;
    }
}
