package org.apache.solr.search.join;

import org.apache.lucene.search.CachingWrapperFilter;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.search.join.ToParentBlockJoinQuery;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.SolrCache;
import org.apache.solr.search.SolrConstantScoreQuery;
import org.apache.solr.search.SyntaxError;

class BlockJoinParentQParser extends QParser {
    private final SolrCache<Query, Filter> parentCache;

    protected String getParentFilterLocalParamName() {
        return "which";
    }

    BlockJoinParentQParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req, SolrCache<Query, Filter> parentCache) {
        super(qstr, localParams, params, req);
        this.parentCache = parentCache;
    }

    @Override
    public Query parse() throws SyntaxError {
        String filter = localParams.get(getParentFilterLocalParamName());
        QParser parentParser = subQuery(filter, null);
        Query parentQ = parentParser.getQuery();
        Filter parentFilter = cachedParentFilter(parentQ);

        String queryText = localParams.get(QueryParsing.V);
        // there is no child query, return parent filter from cache
        if (queryText == null || "".equals(queryText)) {
            SolrConstantScoreQuery wrapped = new SolrConstantScoreQuery(parentFilter);
            wrapped.setCache(false);
            return wrapped;
        }

        QParser childrenParser = subQuery(queryText, null);

        Query childrenQuery = childrenParser.getQuery();
        return createQuery(parentFilter, childrenQuery);
    }

    protected Query createQuery(Filter parentFilter, Query query) {
        return new ToParentBlockJoinQuery(query, parentFilter, ScoreMode.None);// TODO support more scores
    }

    protected Filter cachedParentFilter(Query parentQ) {
      // lazily retrieve from solr cache
      // if no cache then just return wrapped filter
      if (parentCache == null) {
          return createParentFilter(parentQ);
      }

      // lazily retrieve from solr cache
      Filter filter = parentCache.get(parentQ);
      if (filter == null) {
          filter = createParentFilter(parentQ);
          parentCache.put(parentQ, filter);
      }

      return filter;
    }

    protected Filter createParentFilter(Query parentQ) {
        return new CachingWrapperFilter(new QueryWrapperFilter(parentQ) /*,? re-cache dels*/) {
        };
    }
}
