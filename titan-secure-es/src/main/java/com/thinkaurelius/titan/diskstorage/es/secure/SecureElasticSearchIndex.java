package com.thinkaurelius.titan.diskstorage.es.secure;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.core.Mapping;
import com.thinkaurelius.titan.core.Parameter;
import com.thinkaurelius.titan.core.attribute.Cmp;
import com.thinkaurelius.titan.core.attribute.Geo;
import com.thinkaurelius.titan.core.attribute.Geoshape;
import com.thinkaurelius.titan.core.attribute.Text;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import com.thinkaurelius.titan.diskstorage.es.ElasticSearchIndex;
import com.thinkaurelius.titan.diskstorage.indexing.IndexEntry;
import com.thinkaurelius.titan.diskstorage.indexing.IndexMutation;
import com.thinkaurelius.titan.diskstorage.indexing.IndexQuery;
import com.thinkaurelius.titan.diskstorage.indexing.KeyInformation;
import com.thinkaurelius.titan.diskstorage.indexing.RawQuery;
import com.thinkaurelius.titan.diskstorage.indexing.StandardKeyInformation;
import com.thinkaurelius.titan.graphdb.query.TitanPredicate;
import com.thinkaurelius.titan.graphdb.query.condition.And;
import com.thinkaurelius.titan.graphdb.query.condition.Condition;
import com.thinkaurelius.titan.graphdb.query.condition.Not;
import com.thinkaurelius.titan.graphdb.query.condition.Or;
import com.thinkaurelius.titan.graphdb.query.condition.PredicateCondition;
import com.thinkaurelius.titan.graphdb.secure.SecureTransaction;
import com.thinkaurelius.titan.graphdb.secure.SecurityToken;
import ezbake.base.thrift.Authorizations;
import ezbake.base.thrift.Permission;
import ezbake.base.thrift.Visibility;
import ezbake.data.elastic.security.EzSecurityVisibilityFilter;
import ezbake.thrift.ThriftUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.configuration.Configuration;
import org.apache.thrift.TException;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.AndFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.OrFilterBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import static org.elasticsearch.script.ScriptService.ScriptType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class SecureElasticSearchIndex extends ElasticSearchIndex {

    private final Logger log = LoggerFactory.getLogger(SecureElasticSearchIndex.class);

    private final Set<String> stores = Collections.newSetFromMap(new ConcurrentHashMap());
    
    public static final String ES_VISIBILITY_FILTER = "visibility";
    
    public static final String ES_VISIBILITY_FIELD = "visibility";

    public SecureElasticSearchIndex(Configuration config) {
        super(config);
    }

    private StorageException convert(Exception esException) {
        if (esException instanceof InterruptedException) {
            return new TemporaryStorageException("Interrupted while waiting for response", esException);
        } else {
            return new PermanentStorageException("Unknown exception while executing index operation", esException);
        }
    }

    @Override
    public void register(String store, String key, KeyInformation information, TransactionHandle tx) throws StorageException {
        XContentBuilder mapping;
        try {
            mapping = XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject(store)
                    .startObject("_parent")
                    .field("type", parentStore(store))
                    .endObject()
                    .startObject("properties");

            if (!stores.contains(store)) {
                getMapping(mapping, ES_VISIBILITY_FIELD, new StandardKeyInformation(String.class, new Parameter("mapping", Mapping.STRING)));
                stores.add(store);
            }

            getMapping(mapping, key, information);

            mapping.endObject()
                    .endObject()
                    .endObject();

        } catch (IOException e) {
            throw new PermanentStorageException("Could not render json for put mapping request", e);
        }

        try {
            PutMappingResponse response = client.admin().indices().preparePutMapping(indexName).
                    setIgnoreConflicts(false).setType(store).setSource(mapping).execute().actionGet();
        } catch (Exception e) {
            throw convert(e);
        }
    }

    private String parentStore(String store) {
        return store + "_parent";
    }

    @Override
    public void mutate(Map<String, Map<String, IndexMutation>> mutations, KeyInformation.IndexRetriever informations, TransactionHandle tx) throws StorageException {
        Transaction stx = (Transaction) tx;
        String visibility = "";
        if (stx.hasWriteToken()) {
            visibility = stx.getWriteToken().toString();
        }
        BulkRequestBuilder brb = client.prepareBulk();
        int bulkrequests = 0;
        try {
            for (Map.Entry<String, Map<String, IndexMutation>> stores : mutations.entrySet()) {
                String storename = stores.getKey();
                for (Map.Entry<String, IndexMutation> entry : stores.getValue().entrySet()) {
                    String parentid = entry.getKey();
                    String docid = parentid + "|" + visibility;
                    IndexMutation mutation = entry.getValue();
                    Preconditions.checkArgument(!(mutation.isNew() && mutation.isDeleted()));
                    Preconditions.checkArgument(!mutation.isNew() || !mutation.hasDeletions());
                    Preconditions.checkArgument(!mutation.isDeleted() || !mutation.hasAdditions());

                    //Deletions first
                    if (mutation.hasDeletions()) {
                        if (mutation.isDeleted()) {
                            log.trace("Deleting entire document {}", docid);
                            brb.add(new DeleteRequest(indexName, parentStore(storename), parentid));
                            bulkrequests++;
                            client.prepareDeleteByQuery(indexName).setQuery(QueryBuilders.termQuery("_parent", parentid)).execute().actionGet();
                        } else {
                            Set<String> deletions = Sets.newHashSet(mutation.getDeletions());
                            if (mutation.hasAdditions()) {
                                for (IndexEntry ie : mutation.getAdditions()) {
                                    deletions.remove(ie.key);
                                }
                            }
                            if (!deletions.isEmpty()) {
                                //TODO make part of batch mutation if/when possible
                                StringBuilder script = new StringBuilder();
                                for (String key : deletions) {
                                    script.append("ctx._source.remove(\"").append(key).append("\"); ");
                                }
                                log.trace("Deleting individual fields [{}] for document {}", deletions, docid);
                                client.prepareUpdate(indexName, storename, docid).setParent(parentid)
					.setScript(script.toString(), ScriptType.INLINE)
					.execute().actionGet();
                            }
                        }
                    }

                    if (mutation.hasAdditions()) {
                        if (!client.prepareGet(indexName, parentStore(storename), parentid).execute().actionGet().isExists()) {
                            client.prepareIndex(indexName, parentStore(storename), parentid).setSource("{}").execute().actionGet();
                        }
                        List<IndexEntry> entries = Lists.newArrayList(mutation.getAdditions());
                        Visibility ezBakeVis = new Visibility();
                        ezBakeVis.setFormalVisibility(visibility);
                        entries.add(new IndexEntry(ES_VISIBILITY_FIELD, ThriftUtils.serializeToBase64(ezBakeVis)));
                        if (mutation.isNew()) { //Index
                            log.trace("Adding entire document {}", docid);
                            brb.add(new IndexRequest(indexName, storename, docid).parent(parentid).source(getContent(entries)));
                            bulkrequests++;
                        } else { //Update: TODO make part of batch mutation if/when possible
                            if (!client.prepareGet(indexName, storename, docid).setParent(parentid).execute().actionGet().isExists()) {
                                client.prepareIndex(indexName, storename, docid).setParent(parentid).setSource("{}").execute().actionGet();
                            }
                            boolean needUpsert = !mutation.hasDeletions();
                            XContentBuilder builder = getContent(entries);
                            UpdateRequestBuilder update = client.prepareUpdate(indexName, storename, docid).setParent(parentid).setDoc(builder);
                            if (needUpsert) {
                                update.setUpsert(builder);
                            }
                            log.trace("Updating document {} with upsert {}", docid, needUpsert);
                            update.execute().actionGet();
                        }
                    }

                }
            }
            if (bulkrequests > 0) {
                brb.execute().actionGet();
            }
        } catch (Exception e) {
            throw convert(e);
        }
    }

    public FilterBuilder getFilter(String store, Condition<?> condition, KeyInformation.StoreRetriever informations, TransactionHandle tx) {
        if (condition instanceof PredicateCondition) {
            PredicateCondition<String, ?> atom = (PredicateCondition) condition;
            Object value = atom.getValue();
            String key = atom.getKey();
            TitanPredicate titanPredicate = atom.getPredicate();

            FilterBuilder predicateFilter;

            if (value instanceof Number) {
                Preconditions.checkArgument(titanPredicate instanceof Cmp, "Relation not supported on numeric types: " + titanPredicate);
                Cmp numRel = (Cmp) titanPredicate;
                Preconditions.checkArgument(value instanceof Number);

                switch (numRel) {
                    case EQUAL:
                        predicateFilter = FilterBuilders.inFilter(key, value);
                        break;
                    case NOT_EQUAL:
                        predicateFilter = FilterBuilders.notFilter(FilterBuilders.inFilter(key, value));
                        break;
                    case LESS_THAN:
                        predicateFilter = FilterBuilders.rangeFilter(key).lt(value);
                        break;
                    case LESS_THAN_EQUAL:
                        predicateFilter = FilterBuilders.rangeFilter(key).lte(value);
                        break;
                    case GREATER_THAN:
                        predicateFilter = FilterBuilders.rangeFilter(key).gt(value);
                        break;
                    case GREATER_THAN_EQUAL:
                        predicateFilter = FilterBuilders.rangeFilter(key).gte(value);
                        break;
                    default:
                        throw new IllegalArgumentException("Unexpected relation: " + numRel);
                }
            } else if (value instanceof String) {
                Mapping map = Mapping.getMapping(informations.get(key));
                if ((map == Mapping.DEFAULT || map == Mapping.TEXT) && !titanPredicate.toString().startsWith("CONTAINS")) {
                    throw new IllegalArgumentException("Text mapped string values only support CONTAINS queries and not: " + titanPredicate);
                }
                if (map == Mapping.STRING && titanPredicate.toString().startsWith("CONTAINS")) {
                    throw new IllegalArgumentException("String mapped string values do not support CONTAINS queries: " + titanPredicate);
                }

                if (titanPredicate == Text.CONTAINS) {
                    value = ((String) value).toLowerCase();
                    predicateFilter = FilterBuilders.termFilter(key, (String) value);
                } else if (titanPredicate == Text.CONTAINS_PREFIX) {
                    value = ((String) value).toLowerCase();
                    predicateFilter = FilterBuilders.prefixFilter(key, (String) value);
                } else if (titanPredicate == Text.CONTAINS_REGEX) {
                    value = ((String) value).toLowerCase();
                    predicateFilter = FilterBuilders.regexpFilter(key, (String) value);
                } else if (titanPredicate == Text.PREFIX) {
                    predicateFilter = FilterBuilders.prefixFilter(key, (String) value);
                } else if (titanPredicate == Text.REGEX) {
                    predicateFilter = FilterBuilders.regexpFilter(key, (String) value);
                } else if (titanPredicate == Cmp.EQUAL) {
                    predicateFilter = FilterBuilders.termFilter(key, (String) value);
                } else if (titanPredicate == Cmp.NOT_EQUAL) {
                    predicateFilter = FilterBuilders.notFilter(FilterBuilders.termFilter(key, (String) value));
                } else {
                    throw new IllegalArgumentException("Predicate is not supported for string value: " + titanPredicate);
                }
            } else if (value instanceof Geoshape) {
                Preconditions.checkArgument(titanPredicate == Geo.WITHIN, "Relation is not supported for geo value: " + titanPredicate);
                Geoshape shape = (Geoshape) value;
                if (shape.getType() == Geoshape.Type.CIRCLE) {
                    Geoshape.Point center = shape.getPoint();
                    predicateFilter = FilterBuilders.geoDistanceFilter(key).lat(center.getLatitude()).lon(center.getLongitude()).distance(shape.getRadius(), DistanceUnit.KILOMETERS);
                } else if (shape.getType() == Geoshape.Type.BOX) {
                    Geoshape.Point southwest = shape.getPoint(0);
                    Geoshape.Point northeast = shape.getPoint(1);
                    predicateFilter = FilterBuilders.geoBoundingBoxFilter(key).bottomRight(southwest.getLatitude(), northeast.getLongitude()).topLeft(northeast.getLatitude(), southwest.getLongitude());
                } else {
                    throw new IllegalArgumentException("Unsupported or invalid search shape type: " + shape.getType());
                }
            } else {
                throw new IllegalArgumentException("Unsupported type: " + value);
            }

            Authorizations thriftAuths = new Authorizations();
            String esAuths;
            Transaction stx = (Transaction) tx;
            if (stx.hasReadToken()) {
                thriftAuths.setFormalAuthorizations(Sets.newHashSet(stx.getReadToken().toString().split(",")));
            }
            try {
                esAuths = ThriftUtils.serializeToBase64(thriftAuths);
            } catch (TException ex) {
                throw new IllegalStateException("Can't serialize read token auths: " + stx.getReadToken());
            }

            String permissions = Permission.READ.toString();

            FilterBuilder scriptFilter = FilterBuilders.scriptFilter(ES_VISIBILITY_FILTER)
                    .lang("native")
                    .addParam(EzSecurityVisibilityFilter.VISIBILITY_FIELD_PARAM, store + ".visibility")
                    .addParam(EzSecurityVisibilityFilter.REQUIRED_PERMISSIONS_PARAM, permissions)
                    .addParam(EzSecurityVisibilityFilter.AUTHS_PARAM, esAuths);

            return FilterBuilders.hasChildFilter(store, FilterBuilders.andFilter(predicateFilter, scriptFilter));

        } else if (condition instanceof Not) {
            return FilterBuilders.notFilter(getFilter(store, ((Not) condition).getChild(), informations, tx));
        } else if (condition instanceof And) {
            AndFilterBuilder b = FilterBuilders.andFilter();
            for (Condition c : condition.getChildren()) {
                b.add(getFilter(store, c, informations, tx));
            }
            return b;
        } else if (condition instanceof Or) {
            OrFilterBuilder b = FilterBuilders.orFilter();
            for (Condition c : condition.getChildren()) {
                b.add(getFilter(store, c, informations, tx));
            }
            return b;
        } else {
            throw new IllegalArgumentException("Invalid condition: " + condition);
        }
    }

    @Override
    public List<String> query(IndexQuery query, KeyInformation.IndexRetriever informations, TransactionHandle tx) throws StorageException {
        FilterBuilder fb = getFilter(query.getStore(), query.getCondition(), informations.get(query.getStore()), tx);

        SearchRequestBuilder srb = client.prepareSearch(indexName);
        srb.setTypes(parentStore(query.getStore()));
        srb.setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), fb)).setFrom(0);
        if (query.hasLimit()) {
            srb.setSize(query.getLimit());
        } else {
            srb.setSize(maxResultsSize);
        }
        srb.setNoFields();
        //srb.setExplain(true);

        SearchResponse response = srb.execute().actionGet();
        log.debug("Executed query [{}] in {} ms", query.getCondition(), response.getTookInMillis());
        SearchHits hits = response.getHits();
        if (!query.hasLimit() && hits.totalHits() >= maxResultsSize) {
            log.warn("Query result set truncated to first [{}] elements for query: {}", maxResultsSize, query);
        }
        List<String> result = new ArrayList<String>(hits.hits().length);
        for (SearchHit hit : hits) {
            result.add(hit.id());
        }
        return result;
    }

    @Override
    public Iterable<RawQuery.Result<String>> query(RawQuery query, KeyInformation.IndexRetriever informations, TransactionHandle tx) throws StorageException {
        throw new UnsupportedOperationException("Secure ES doesn't support raw queries.");
    }

    @Override
    public TransactionHandle beginTransaction() throws StorageException {
        return new Transaction();

    }

    private class Transaction implements TransactionHandle, SecureTransaction {

        SecurityToken.ReadToken readToken;
        SecurityToken.WriteToken writeToken;

        @Override
        public void commit() throws StorageException {
        }

        @Override
        public void rollback() throws StorageException {
        }

        @Override
        public void flush() throws StorageException {
        }

        @Override
        public boolean hasReadToken() {
            return readToken != null;
        }

        @Override
        public boolean hasWriteToken() {
            return writeToken != null;
        }

        @Override
        public SecurityToken.ReadToken getReadToken() {
            return readToken;
        }

        @Override
        public SecurityToken.WriteToken getWriteToken() {
            Preconditions.checkState(writeToken != null);
            return writeToken;
        }

        @Override
        public void setReadToken(SecurityToken.ReadToken readToken) {
            Preconditions.checkArgument(readToken != null);
            this.readToken = readToken;
        }

        @Override
        public void setWriteToken(SecurityToken.WriteToken writeToken) {
            Preconditions.checkArgument(writeToken != null);
            this.writeToken = writeToken;
        }
    }
}
