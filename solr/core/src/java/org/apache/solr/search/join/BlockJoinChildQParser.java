package org.apache.solr.search.join;

import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.ToChildBlockJoinQuery;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.SolrCache;

public class BlockJoinChildQParser extends BlockJoinParentQParser {

    public BlockJoinChildQParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req, SolrCache parentCache) {
        super(qstr, localParams, params, req, parentCache);
    }

    @Override
    protected Query createQuery(Filter parentFilter, Query query) {
        return new ToChildBlockJoinQuery(query, parentFilter, false);// TODO support scores
    }

    @Override
    protected String getParentFilterLocalParamName() {
        return "of";
    }
}
