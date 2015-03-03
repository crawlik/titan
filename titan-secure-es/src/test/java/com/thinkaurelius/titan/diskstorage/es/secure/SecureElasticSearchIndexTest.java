package com.thinkaurelius.titan.diskstorage.es.secure;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.thinkaurelius.titan.core.Mapping;
import com.thinkaurelius.titan.core.Parameter;
import com.thinkaurelius.titan.core.attribute.Cmp;
import com.thinkaurelius.titan.core.attribute.Geo;
import com.thinkaurelius.titan.core.attribute.Geoshape;
import com.thinkaurelius.titan.core.attribute.Text;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.accumulo.AccumuloSecurityToken;
import com.thinkaurelius.titan.diskstorage.es.ElasticSearchIndex;
import com.thinkaurelius.titan.diskstorage.indexing.IndexProvider;
import static com.thinkaurelius.titan.diskstorage.indexing.IndexProviderTest.LOCATION;
import static com.thinkaurelius.titan.diskstorage.indexing.IndexProviderTest.TEXT;
import static com.thinkaurelius.titan.diskstorage.indexing.IndexProviderTest.TIME;
import static com.thinkaurelius.titan.diskstorage.indexing.IndexProviderTest.WEIGHT;
import static com.thinkaurelius.titan.diskstorage.indexing.IndexProviderTest.getDocument;
import static com.thinkaurelius.titan.diskstorage.indexing.IndexProviderTest.indexRetriever;
import com.thinkaurelius.titan.diskstorage.indexing.IndexQuery;
import com.thinkaurelius.titan.diskstorage.indexing.SecureIndexProviderTest;
import com.thinkaurelius.titan.graphdb.query.condition.And;
import com.thinkaurelius.titan.graphdb.query.condition.Or;
import com.thinkaurelius.titan.graphdb.query.condition.PredicateCondition;
import com.thinkaurelius.titan.graphdb.secure.SecureIndexTransaction;
import java.util.List;
import java.util.Map;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class SecureElasticSearchIndexTest extends SecureIndexProviderTest {

    public static final String PUBLIC_VIS = "PUBLIC";
    public static final String PRIVATE_VIS = "PRIVATE";

    @Override
    public void open() throws StorageException {
        index = openIndex();
        tx = new SecureIndexTransaction(index, indexRetriever);
    }

    @Override
    public IndexProvider openIndex() throws StorageException {
        return new SecureElasticSearchIndex(getLocalESTestConfig());
    }

    @Override
    public boolean supportsLuceneStyleQueries() {
        return false;
    }

    @Override
    public boolean supportsOrderingQueries() {
        return false;
    }

    public static final Configuration getLocalESTestConfig() {
        Configuration config = new BaseConfiguration();
        config.setProperty(ElasticSearchIndex.LOCAL_MODE_KEY, true);
        config.setProperty(ElasticSearchIndex.CLIENT_ONLY_KEY, false);
        config.setProperty(ElasticSearchIndex.ES_YML_KEY, "elasticsearch.yml");
        return config;
    }

    @Test
    public void testSupport() {
        assertTrue(index.supports(of(String.class)));
        assertTrue(index.supports(of(String.class, new Parameter("mapping", Mapping.TEXT))));
        assertTrue(index.supports(of(String.class, new Parameter("mapping", Mapping.STRING))));

        assertTrue(index.supports(of(Double.class)));
        assertFalse(index.supports(of(Double.class, new Parameter("mapping", Mapping.TEXT))));

        assertTrue(index.supports(of(Long.class)));
        assertTrue(index.supports(of(Long.class, new Parameter("mapping", Mapping.DEFAULT))));
        assertTrue(index.supports(of(Integer.class)));
        assertTrue(index.supports(of(Short.class)));
        assertTrue(index.supports(of(Byte.class)));
        assertTrue(index.supports(of(Float.class)));
        assertTrue(index.supports(of(Geoshape.class)));
        assertFalse(index.supports(of(Object.class)));
        assertFalse(index.supports(of(Exception.class)));

        assertTrue(index.supports(of(String.class), Text.CONTAINS));
        assertTrue(index.supports(of(String.class, new Parameter("mapping", Mapping.TEXT)), Text.CONTAINS_PREFIX));
        assertTrue(index.supports(of(String.class, new Parameter("mapping", Mapping.TEXT)), Text.CONTAINS_REGEX));
        assertFalse(index.supports(of(String.class, new Parameter("mapping", Mapping.TEXT)), Text.REGEX));
        assertFalse(index.supports(of(String.class, new Parameter("mapping", Mapping.STRING)), Text.CONTAINS));
        assertTrue(index.supports(of(String.class, new Parameter("mapping", Mapping.STRING)), Text.PREFIX));
        assertTrue(index.supports(of(String.class, new Parameter("mapping", Mapping.STRING)), Text.REGEX));
        assertTrue(index.supports(of(String.class, new Parameter("mapping", Mapping.STRING)), Cmp.EQUAL));
        assertTrue(index.supports(of(String.class, new Parameter("mapping", Mapping.STRING)), Cmp.NOT_EQUAL));

        assertTrue(index.supports(of(Double.class), Cmp.EQUAL));
        assertTrue(index.supports(of(Double.class), Cmp.GREATER_THAN_EQUAL));
        assertTrue(index.supports(of(Double.class), Cmp.LESS_THAN));
        assertTrue(index.supports(of(Double.class, new Parameter("mapping", Mapping.DEFAULT)), Cmp.LESS_THAN));
        assertFalse(index.supports(of(Double.class, new Parameter("mapping", Mapping.TEXT)), Cmp.LESS_THAN));
        assertTrue(index.supports(of(Geoshape.class), Geo.WITHIN));

        assertFalse(index.supports(of(Double.class), Geo.INTERSECT));
        assertFalse(index.supports(of(Long.class), Text.CONTAINS));
        assertFalse(index.supports(of(Geoshape.class), Geo.DISJOINT));
    }

    @Test
    public void singleSecureStore() throws Exception {
        secureStoreTest("vertex");
    }

    public void setWriteToken(String visibility) {
        ((SecureIndexTransaction) tx).setWriteToken(AccumuloSecurityToken.getInstance().newWriteToken(visibility));
    }

    public void setReadToken(String authorizations) {
        ((SecureIndexTransaction) tx).setReadToken(AccumuloSecurityToken.getInstance().newReadToken(authorizations));
    }

    protected void secureStoreTest(String... stores) throws Exception {

        Map<String, Object> doc1 = getDocument("Hello world", 1001, 5.2, Geoshape.point(48.0, 0.0));

        setWriteToken(PUBLIC_VIS);

        for (String store : stores) {
            initialize(store);
            add(store, "doc1", doc1, true);

        }

        clopen();

        for (String store : stores) {

            List<String> result = tx.query(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS, "world")));
            assertEquals(ImmutableSet.of(), ImmutableSet.copyOf(result));

            setReadToken(PUBLIC_VIS);
            result = tx.query(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS, "world")));
            assertEquals(ImmutableSet.of("doc1"), ImmutableSet.copyOf(result));

            setReadToken(PRIVATE_VIS);
            result = tx.query(new IndexQuery(store, PredicateCondition.of(TEXT, Text.CONTAINS, "world")));
            assertEquals(ImmutableSet.of(), ImmutableSet.copyOf(result));

            setWriteToken(PRIVATE_VIS);
            add(store, "doc1", ImmutableMap.of(TIME, (Object) 1005), false);
        }

        clopen();

        for (String store : stores) {

            setReadToken(PUBLIC_VIS);
            List<String> result = tx.query(new IndexQuery(store, PredicateCondition.of(TIME, Cmp.GREATER_THAN, 1001)));
            assertEquals(ImmutableSet.of(), ImmutableSet.copyOf(result));

            result = tx.query(new IndexQuery(store,
                    And.of(PredicateCondition.of(TEXT, Text.CONTAINS, "world"),
                            PredicateCondition.of(TIME, Cmp.GREATER_THAN, 1001))));
            assertEquals(ImmutableSet.of(), ImmutableSet.copyOf(result));

            result = tx.query(new IndexQuery(store,
                    And.of(Or.of(PredicateCondition.of(TEXT, Text.CONTAINS, "world"),
                                    PredicateCondition.of(TIME, Cmp.GREATER_THAN, 1001)))));
            assertEquals(ImmutableSet.of("doc1"), ImmutableSet.copyOf(result));

            setReadToken(PRIVATE_VIS);
            result = tx.query(new IndexQuery(store, PredicateCondition.of(TIME, Cmp.GREATER_THAN, 1001)));
            assertEquals(ImmutableSet.of("doc1"), ImmutableSet.copyOf(result));

            result = tx.query(new IndexQuery(store,
                    And.of(Or.of(PredicateCondition.of(TEXT, Text.CONTAINS, "world"),
                                    PredicateCondition.of(TIME, Cmp.GREATER_THAN, 1001)))));
            assertEquals(ImmutableSet.of("doc1"), ImmutableSet.copyOf(result));

            setWriteToken(PUBLIC_VIS + "|" + PRIVATE_VIS);
            add(store, "doc1", ImmutableMap.of(WEIGHT, (Object) 11.1), false);
        }

        clopen();

        for (String store : stores) {
            List<String> result = tx.query(new IndexQuery(store, PredicateCondition.of(WEIGHT, Cmp.GREATER_THAN, 11.0)));
            assertEquals(ImmutableSet.of(), ImmutableSet.copyOf(result));

            setReadToken(PUBLIC_VIS);
            result = tx.query(new IndexQuery(store, PredicateCondition.of(WEIGHT, Cmp.GREATER_THAN, 11.0)));
            assertEquals(ImmutableSet.of("doc1"), ImmutableSet.copyOf(result));

            setReadToken(PRIVATE_VIS);
            result = tx.query(new IndexQuery(store, PredicateCondition.of(WEIGHT, Cmp.GREATER_THAN, 11.0)));
            assertEquals(ImmutableSet.of("doc1"), ImmutableSet.copyOf(result));

            setWriteToken(PUBLIC_VIS + "&" + PRIVATE_VIS);
            add(store, "doc1", ImmutableMap.of(TIME, (Object) 1005, WEIGHT, 11.1), false);
        }

        clopen();

        for (String store : stores) {
            List<String> result = tx.query(new IndexQuery(store, PredicateCondition.of(WEIGHT, Cmp.GREATER_THAN, 11.0)));
            assertEquals(ImmutableSet.of(), ImmutableSet.copyOf(result));

            setReadToken(PUBLIC_VIS);
            result = tx.query(new IndexQuery(store, PredicateCondition.of(WEIGHT, Cmp.GREATER_THAN, 11.0)));
            assertEquals(ImmutableSet.of("doc1"), ImmutableSet.copyOf(result));

            setReadToken(PRIVATE_VIS);
            result = tx.query(new IndexQuery(store, PredicateCondition.of(WEIGHT, Cmp.GREATER_THAN, 11.0)));
            assertEquals(ImmutableSet.of("doc1"), ImmutableSet.copyOf(result));

            setWriteToken(PUBLIC_VIS + "&" + PRIVATE_VIS);
            add(store, "doc1", ImmutableMap.of(LOCATION, (Object) Geoshape.point(-48.0, 0.0)), false);
        }

        clopen();

        for (String store : stores) {
            List<String> result = tx.query(new IndexQuery(store, PredicateCondition.of(LOCATION, Geo.WITHIN, Geoshape.circle(-48.5, 0.5, 200.00))));
            assertEquals(ImmutableSet.of(), ImmutableSet.copyOf(result));

            setReadToken(PUBLIC_VIS);
            result = tx.query(new IndexQuery(store, PredicateCondition.of(LOCATION, Geo.WITHIN, Geoshape.circle(-48.5, 0.5, 200.00))));
            assertEquals(ImmutableSet.of(), ImmutableSet.copyOf(result));

            setReadToken(PRIVATE_VIS);
            result = tx.query(new IndexQuery(store, PredicateCondition.of(LOCATION, Geo.WITHIN, Geoshape.circle(-48.5, 0.5, 200.00))));
            assertEquals(ImmutableSet.of(), ImmutableSet.copyOf(result));

            setReadToken(PUBLIC_VIS + "," + PRIVATE_VIS);
            result = tx.query(new IndexQuery(store, PredicateCondition.of(LOCATION, Geo.WITHIN, Geoshape.circle(-48.5, 0.5, 200.00))));
            assertEquals(ImmutableSet.of("doc1"), ImmutableSet.copyOf(result));
        }
    }
}
