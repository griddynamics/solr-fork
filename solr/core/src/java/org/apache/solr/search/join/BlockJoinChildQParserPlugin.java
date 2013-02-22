package org.apache.solr.search.join;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SolrCache;

/**
 * Usage: {!child of="PARENT:true"}PARENT_PRICE:10
 * 
 * To obtain just parent filter from the cache, omit child query: {!child of="PARENT:true"}
 * 
 * This parser needs user cache to store parent filter as CachingWrapperFilter provide it's name via tag in
 * solrconfig.xml:
 * 
 * <str name="parent-filter-cache">blockJoinParentFilterCache</str>
 * 
 * no default value is provided for cache name. User cache with this name should be configured, otherwise query might
 * not be performant
 **/
public class BlockJoinChildQParserPlugin extends BlockJoinParentQParserPlugin {

    @Override
    protected QParser createBJQParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req, SolrCache parentCache) {
        return new BlockJoinChildQParser(qstr, localParams, params, req, parentCache);
    }
}
