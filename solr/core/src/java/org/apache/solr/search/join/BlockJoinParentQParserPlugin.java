package org.apache.solr.search.join;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.SolrCache;

/**
 * Usage: {!parent which="PARENT:true"}CHILD_PRICE:10
 * 
 * To obtain just parent filter from the cache, omit child query: {!parent which="PARENT:true"}
 * 
 * This parser needs user cache to store parent filter as CachingWrapperFilter provide it's name via tag in
 * solrconfig.xml:
 * 
 * <str name="parent-filter-cache">blockJoinParentFilterCache</str>
 * 
 * no default value is provided for cache name. User cache with this name should be configured, otherwise query might
 * not be performant
 **/
public class BlockJoinParentQParserPlugin extends QParserPlugin {

    private String parentCacheName;

    @Override
    public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {

        final SolrCache parentCache = (parentCacheName == null) ? null : req.getSearcher().getCache(parentCacheName);

        QParser parser = createBJQParser(qstr, localParams, params, req, parentCache);

        return parser;
    }

    protected QParser createBJQParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req,
            final SolrCache parentCache) {
        return new BlockJoinParentQParser(qstr, localParams, params, req, parentCache);
    }

    @Override
    public void init(NamedList args) {

        if (args != null) {
            parentCacheName = (String) args.get("parent-filter-cache");
        }
    }

}
