package com.thinkaurelius.titan.graphdb;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.graphdb.secure.SecureGraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.secure.SecureTitanFactory;
import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.groovy.Gremlin;
import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.iterators.SingleIterator;
import java.util.List;
import java.util.Set;
import org.apache.commons.configuration.Configuration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author edeprit@42six.com
 */
public abstract class SecureTitanGraphTest extends TitanGraphTestCommon {

    private final Logger log = LoggerFactory.getLogger(SecureTitanGraphTest.class);

    private static final String VERTEX_KEY = "vertexKey";
    private static final String VERTEX_VAL = "vertexVal";
    private static final String PUBLIC_KEY = "publicKey";
    private static final String PUBLIC_VAL = "publicVal";
    private static final String PRIVATE_KEY = "privateKey";
    private static final String PRIVATE_VAL = "privateVal";
    private static final String PUBLIC_PRIVATE_KEY = "publicPrivateKey";
    private static final String PUBLIC_PRIVATE_VAL = "publicPrivateVal";
    private static final String EDGE_KEY = "edgeKey";

    public SecureTitanGraphTest(Configuration config) {
        super(config);
    }

    public enum WriteToken {

        NULL(""),
        PUBLIC("PUBLIC"),
        PRIVATE("PRIVATE"),
        PUBLIC_OR_PRIVATE("PUBLIC|PRIVATE"),
        PUBLIC_AND_PRIVATE("PUBLIC&PRIVATE");

        private final String text;

        private WriteToken(final String text) {
            this.text = text;
        }

        @Override
        public String toString() {
            return text;
        }
    }

    public enum ReadToken {

        NULL(""),
        PUBLIC("PUBLIC"),
        PRIVATE("PRIVATE"),
        PUBLIC_AND_PRIVATE("PUBLIC,PRIVATE");

        private final String text;

        private ReadToken(final String text) {
            this.text = text;
        }

        @Override
        public String toString() {
            return text;
        }
    }

    @Override
    @Before
    public void setUp() throws Exception {
        SecureGraphDatabaseConfiguration graphconfig = new SecureGraphDatabaseConfiguration(config);
        graphconfig.getBackend().clearStorage();
        open();
        defineSchema();
    }

    @Override
    public void open() {
        graph = SecureTitanFactory.open(config);
        tx = graph.newTransaction();
    }

    public abstract void startReadTx(ReadToken token);

    public abstract void startWriteTx(WriteToken token);

    public abstract void startReadWriteTx(ReadToken readToken, WriteToken writeToken);

    public void defineSchema() {
        graph.makeKey(VERTEX_KEY).indexed(Vertex.class).dataType(String.class).list().make();
        graph.makeKey(PUBLIC_KEY).indexed(Vertex.class).dataType(String.class).list().make();
        graph.makeKey(PRIVATE_KEY).indexed(Vertex.class).dataType(String.class).list().make();
        graph.makeKey(PUBLIC_PRIVATE_KEY).indexed(Vertex.class).dataType(String.class).list().make();

        graph.makeKey(EDGE_KEY).indexed(Vertex.class).dataType(String.class).make();

        graph.makeLabel("knows").make();

        graph.commit();
    }

    @Test
    public void testMakeKey() {
        TitanKey key = makeStringPropertyKey("newKey");
        clopen();
        assertTrue(tx.containsType("newKey"));

        startWriteTx(WriteToken.PUBLIC);
        try {
            key = makeStringPropertyKey("newerKey");
            fail();
        } catch (IllegalStateException exc) {
        }
    }

    @Test
    public void testMakeLabel() {
        TitanLabel label = makeSimpleEdgeLabel("newLabel");
        clopen();
        assertTrue(tx.containsType("newLabel"));

        startWriteTx(WriteToken.PUBLIC);
        try {
            label = makeSimpleEdgeLabel("newerLabel");
            fail();
        } catch (IllegalStateException exc) {
        }
    }

    @Test
    public void testAddVertex() {
        TitanVertex v = tx.addVertex();
        clopen();
        assertTrue(tx.containsVertex(v.getID()));

        startWriteTx(WriteToken.PUBLIC);
        try {
            v = tx.addVertex();
            fail();
        } catch (IllegalStateException exc) {
        }
    }

    @Test
    public void testRemoveVertex() {
        TitanVertex v = tx.addVertex();
        clopen();
        assertTrue(tx.containsVertex(v.getID()));

        try {
            tx.removeVertex(v);
            fail();
        } catch (TitanException exc) {
        }
        
        graph.removeVertex(graph.getVertex(v));
        graph.commit();
        clopen();
        Iterable<Vertex> vlist = graph.getVertices();
        assertTrue(Iterables.isEmpty(vlist));
    }

    @Test
    public void testImmutableEdge() {
        TitanVertex v1 = tx.addVertex();
        TitanVertex v2 = tx.addVertex();
        TitanEdge e = (TitanEdge) tx.addEdge(null, v1, v2, "knows");
        e.setProperty(EDGE_KEY, "val1");
        clopen();
        e = (TitanEdge) Iterables.getOnlyElement(tx.getEdges());
        assertEquals("val1", e.getProperty(EDGE_KEY));

        try {
            e.setProperty(EDGE_KEY, "val2");
            clopen();
            fail();
        } catch (TitanException exc) {
            tx.rollback();
            clopen();
        }

        e = (TitanEdge) Iterables.getOnlyElement(tx.getEdges());
        tx.removeEdge(e);
        clopen();
        assertEquals(0, Iterables.size(tx.getEdges()));
    }

    @Test
    public void testRemoveEdge() {
        TitanVertex v1 = tx.addVertex();
        TitanVertex v2 = tx.addVertex();
        clopen();

        startWriteTx(WriteToken.PRIVATE);
        Edge e = tx.addEdge(null, tx.getVertex(v1), tx.getVertex(v2), "knows");
        clopen();

        startReadWriteTx(ReadToken.PRIVATE, WriteToken.PRIVATE);
        List<Edge> es = Lists.newArrayList(tx.getEdges());
        assertEquals(1, es.size());

        e = es.get(0);
        tx.removeEdge(e);
        clopen();

        startReadTx(ReadToken.PRIVATE);
        es = Lists.newArrayList(tx.getEdges());
        assertTrue(es.isEmpty());
    }

    /**
     * This method ensures that the same property/key can be stored with
     * different visibilities on the same vertex as individual Accumulo rows in
     * the backend and still be queried on as expected with visibility.
     */
    @Test
    public void testUniqueConstraint() {
        // create new vertex
        Vertex v1 = tx.addVertex(null);
        tx.commit();

        // add same key/value using three different visibilities
        startWriteTx(WriteToken.PUBLIC);
        TitanVertex tv = (TitanVertex) tx.getVertex(v1);
        tv.addProperty(VERTEX_KEY, VERTEX_VAL);
        tx.commit();

        startWriteTx(WriteToken.PRIVATE);
        tv = (TitanVertex) tx.getVertex(v1);
        tv.addProperty(VERTEX_KEY, VERTEX_VAL);
        tx.commit();

        startWriteTx(WriteToken.PUBLIC_AND_PRIVATE);
        tv = (TitanVertex) tx.getVertex(v1);
        tv.addProperty(VERTEX_KEY, VERTEX_VAL);
        clopen();

        // access values with respective authorities
        startReadTx(ReadToken.NULL);
        tv = (TitanVertex) tx.getVertex(v1);
        List<String> props = (List<String>) tv.getProperty(VERTEX_KEY);
        assertTrue(props.isEmpty());
        tx.commit();

        startReadTx(ReadToken.PUBLIC);
        tv = (TitanVertex) tx.getVertex(v1);
        props = (List<String>) tv.getProperty(VERTEX_KEY);
        assertEquals(1, props.size());
        tx.commit();

        startReadTx(ReadToken.PRIVATE);
        tv = (TitanVertex) tx.getVertex(v1);
        props = (List<String>) tv.getProperty(VERTEX_KEY);
        assertEquals(1, props.size());
        tx.commit();

        startReadTx(ReadToken.PUBLIC_AND_PRIVATE);
        tv = (TitanVertex) tx.getVertex(v1);
        props = (List<String>) tv.getProperty(VERTEX_KEY);
        assertEquals(3, props.size());
        tx.commit();

        startReadTx(ReadToken.PUBLIC);
        Iterable<Vertex> vlist = tx.getVertices(VERTEX_KEY, VERTEX_VAL);
        assertEquals(1, Iterables.size(vlist));
        assertEquals(VERTEX_VAL, ((List) Iterables.getOnlyElement(vlist).getProperty(VERTEX_KEY)).get(0));
        vlist = tx.query().has(VERTEX_KEY, Compare.EQUAL, VERTEX_VAL).vertices();
        assertEquals(1, Iterables.size(vlist));
        assertEquals(VERTEX_VAL, ((List) Iterables.getOnlyElement(vlist).getProperty(VERTEX_KEY)).get(0));
        tx.commit();
    }

    /**
     * Creates a vertex and sets properties with varying visibilities. Tries to
     * read them back with respective Authorities.
     */
    @Test
    public void testVertexWithMultipleVisibility() {

        // first add a vertex and a property with no visibility
        startWriteTx(WriteToken.NULL);
        TitanVertex v1 = tx.addVertex();
        v1.addProperty(VERTEX_KEY, VERTEX_VAL);
        tx.commit();

        // now add properties with different visibilities
        startWriteTx(WriteToken.PUBLIC);
        TitanVertex v = (TitanVertex) tx.getVertex(v1);
        v.addProperty(PUBLIC_KEY, PUBLIC_VAL);
        tx.commit();

        startWriteTx(WriteToken.PRIVATE);
        v = (TitanVertex) tx.getVertex(v1);
        v.addProperty(PRIVATE_KEY, PRIVATE_VAL);
        tx.commit();

        startWriteTx(WriteToken.PUBLIC_AND_PRIVATE);
        v = (TitanVertex) tx.getVertex(v1);
        v.addProperty(PUBLIC_PRIVATE_KEY, PUBLIC_PRIVATE_VAL);
        clopen();

        // ensure no authorities retrieves only unclass value
        v = (TitanVertex) tx.getVertex(v1);
        assertEquals(1, v.getPropertyKeys().size());
        assertEquals(VERTEX_VAL, ((List) v.getProperty(VERTEX_KEY)).get(0));

        // get only with 42six authority
        startReadTx(ReadToken.PUBLIC);
        v = (TitanVertex) tx.getVertex(v1);
        assertEquals(2, v.getPropertyKeys().size());
        assertEquals(VERTEX_VAL, ((List) v.getProperty(VERTEX_KEY)).get(0));
        assertEquals(PUBLIC_VAL, ((List) v.getProperty(PUBLIC_KEY)).get(0));
        tx.commit();

        // get only with CSC authority
        startReadTx(ReadToken.PRIVATE);
        v = (TitanVertex) tx.getVertex(v1);
        assertEquals(2, v.getPropertyKeys().size());
        assertEquals(VERTEX_VAL, ((List) v.getProperty(VERTEX_KEY)).get(0));
        assertEquals(PRIVATE_VAL, ((List) v.getProperty(PRIVATE_KEY)).get(0));
        tx.commit();

        // get with CSC & 42six authority
        startReadTx(ReadToken.PUBLIC_AND_PRIVATE);
        v = (TitanVertex) tx.getVertex(v1);
        assertEquals(4, v.getPropertyKeys().size());
        assertEquals(VERTEX_VAL, ((List) v.getProperty(VERTEX_KEY)).get(0));
        assertEquals(PUBLIC_VAL, ((List) v.getProperty(PUBLIC_KEY)).get(0));
        assertEquals(PRIVATE_VAL, ((List) v.getProperty(PRIVATE_KEY)).get(0));
        assertEquals(PUBLIC_PRIVATE_VAL, ((List) v.getProperty(PUBLIC_PRIVATE_KEY)).get(0));
        tx.commit();
    }

    /**
     * Tests edge traversal with visibility. Creates three vertices and two
     * edges. Each edge has a different visibility. Starting with center vertex,
     * we attempt to get adjacent edges using different visibilities.
     */
    @Test
    public void testEdgeTraversalVisibility() {

        // add vertices and an edge using 42six visibility
        // note: the vertices are always created without visibility by necessity
        startWriteTx(WriteToken.NULL);
        TitanVertex v1 = (TitanVertex) tx.addVertex(null);
        TitanVertex v2 = (TitanVertex) tx.addVertex(null);
        TitanVertex v3 = (TitanVertex) tx.addVertex(null);
        tx.commit();

        startWriteTx(WriteToken.PUBLIC);
        Edge edge1 = tx.addEdge(null, tx.getVertex(v1), tx.getVertex(v2), "knows");
        tx.commit();

        // add edge with CSC visibility
        startWriteTx(WriteToken.PRIVATE);
        Edge edge2 = tx.addEdge(null, tx.getVertex(v2), tx.getVertex(v3), "knows");
        clopen();

        // ensure from center vertex, only the left edge is traversable
        startReadTx(ReadToken.PUBLIC);
        Vertex v = tx.getVertex(v2);
        Iterable<Vertex> vs = v.getVertices(Direction.BOTH, "knows");
        assertEquals(1, Iterables.size(vs));
        assertEquals(v1.getId(), Iterables.getOnlyElement(vs).getId());
        tx.commit();

        // ensure from center vertex, only the right edge is traversable
        startReadTx(ReadToken.PRIVATE);
        v = tx.getVertex(v2);
        Iterable<Edge> es = v.getEdges(Direction.BOTH, "knows");
        assertEquals(1, Iterables.size(es));
        assertEquals(edge2.getId(), Iterables.getOnlyElement(es).getId());
        tx.commit();

        startReadTx(ReadToken.PUBLIC_AND_PRIVATE);
        v = tx.getVertex(v2);
        es = v.getEdges(Direction.BOTH, "knows");
        assertEquals(2, Iterables.size(es));
        tx.commit();
    }

    /**
     * Tests vertex property visibility starting with an edge.
     */
    @Test
    public void testVertexVisibilityFromEdge() {
        startWriteTx(WriteToken.NULL);
        TitanVertex v1 = (TitanVertex) tx.addVertex(null);
        TitanVertex v2 = (TitanVertex) tx.addVertex(null);
        TitanVertex v3 = (TitanVertex) tx.addVertex(null);
        tx.commit();

        startWriteTx(WriteToken.PUBLIC);
        ((TitanVertex) tx.getVertex(v3)).addProperty(PUBLIC_KEY, PUBLIC_VAL);
        tx.commit();

        startWriteTx(WriteToken.PRIVATE);
        ((TitanVertex) tx.getVertex(v3)).addProperty(PRIVATE_KEY, PRIVATE_VAL);
        tx.commit();

        startWriteTx(WriteToken.PUBLIC);
        Edge edge1 = tx.addEdge(null, tx.getVertex(v1), tx.getVertex(v2), "knows");
        tx.commit();

        startWriteTx(WriteToken.PRIVATE);
        Edge edge2 = tx.addEdge(null, tx.getVertex(v2), tx.getVertex(v3), "knows");
        clopen();

        startReadTx(ReadToken.PRIVATE);
        Edge edge = tx.getEdge(edge2.getId());
        Vertex v = edge.getVertex(Direction.IN);
        Set<String> keys = v.getPropertyKeys();
        assertEquals(1, keys.size());
        assertEquals(PRIVATE_VAL, ((List) v.getProperty(PRIVATE_KEY)).get(0));
        v = edge.getVertex(Direction.OUT);
        assertTrue(v.getPropertyKeys().isEmpty());
        tx.commit();
    }

    /**
     * Test out visibility with query service
     */
    @Test
    public void testGraphQuery() {
        startWriteTx(WriteToken.NULL);
        TitanVertex v1 = (TitanVertex) tx.addVertex(null);
        TitanVertex v2 = (TitanVertex) tx.addVertex(null);
        TitanVertex v3 = (TitanVertex) tx.addVertex(null);
        tx.commit();

        startWriteTx(WriteToken.PUBLIC);
        TitanVertex tv = (TitanVertex) tx.getVertex(v1);
        tv.addProperty(VERTEX_KEY, PUBLIC_VAL);
        tx.commit();

        startWriteTx(WriteToken.PRIVATE);
        tv = (TitanVertex) tx.getVertex(v2);
        tv.addProperty(VERTEX_KEY, PRIVATE_VAL);
        tx.commit();

        startWriteTx(WriteToken.PUBLIC_AND_PRIVATE);
        tv = (TitanVertex) tx.getVertex(v3);
        tv.addProperty(VERTEX_KEY, PUBLIC_PRIVATE_VAL);
        clopen();

        startReadTx(ReadToken.NULL);
        assertTrue(Iterables.isEmpty(tx.query().has(VERTEX_KEY).vertices()));

        startReadTx(ReadToken.PUBLIC);
        Iterable<Vertex> vlist = tx.query().has(VERTEX_KEY).vertices();
        assertEquals(1, Iterables.size(vlist));
        tx.commit();

        startReadTx(ReadToken.PRIVATE);
        vlist = tx.query().has(VERTEX_KEY).vertices();
        assertEquals(1, Iterables.size(vlist));
        tx.commit();

        startReadTx(ReadToken.PUBLIC_AND_PRIVATE);
        vlist = tx.query().has(VERTEX_KEY).vertices();
        assertEquals(3, Iterables.size(vlist));
        vlist = tx.query().has(VERTEX_KEY, Compare.EQUAL, PUBLIC_VAL).vertices();
        assertEquals(1, Iterables.size(vlist));
        vlist = tx.query().has(VERTEX_KEY, Compare.NOT_EQUAL, PUBLIC_VAL).vertices();
        assertEquals(2, Iterables.size(vlist));
        tx.commit();
    }

    /**
     * Tests to ensure that a cached query/result under one visibility is not
     * viewable by a transaction of a differnt authority.
     */
    @Test
    public void testIndexCache() {
        // add some vertices to query on
        startWriteTx(WriteToken.NULL);
        TitanVertex v1 = tx.addVertex(null);
        tx.commit();

        startWriteTx(WriteToken.PUBLIC);
        v1 = (TitanVertex) tx.getVertex(v1);
        v1.addProperty(PUBLIC_KEY, PUBLIC_VAL);
        tx.commit();

        // run a simple query so that the results should be cached in the index cache since it's the same query and result
        startReadTx(ReadToken.PUBLIC);
        Iterable<Vertex> vv = tx.query().has(PUBLIC_KEY, Compare.EQUAL, PUBLIC_VAL).vertices();
        assertEquals(1, Iterables.size(vv));
        tx.commit();

        // start a new transaction with different authority and run the same query, hopefully the new authority would not recall
        // cached value
        startWriteTx(WriteToken.PRIVATE);
        Iterable<Vertex> list = tx.query().has(PUBLIC_KEY, Compare.EQUAL, PUBLIC_VAL).vertices();
        assertTrue(Iterables.isEmpty(list));
        tx.commit();
    }

    /**
     * Simple test to see if Gremlin queries will work with visibility for edge
     * traversal and property visibility.
     */
    @Test
    public void testGremlinVisibilityQuery() {
        startWriteTx(WriteToken.NULL);
        TitanVertex v1 = tx.addVertex(null);
        TitanVertex v2 = tx.addVertex(null);
        tx.commit();

        startWriteTx(WriteToken.PRIVATE);
        v1 = (TitanVertex) tx.getVertex(v1);
        v1.addProperty(VERTEX_KEY, VERTEX_VAL);
        tx.addEdge((TitanVertex) tx.getVertex(v1), (TitanVertex) tx.getVertex(v2), "knows");
        clopen();

        startReadTx(ReadToken.PRIVATE);
        Vertex v = tx.getVertex(v1);

        Pipe pipe = Gremlin.compile("_().out('knows').id");
        boolean called = false;
        pipe.setStarts(new SingleIterator<Vertex>(v));
        for (Object name : pipe) {
            called = true;
            assertEquals(v2.getId().toString(), name.toString());
        }
        assertTrue(called);

        pipe = Gremlin.compile("_()." + VERTEX_KEY);
        pipe.setStarts(new SingleIterator<Vertex>(v));
        for (Object name : pipe) {
            if (name != null) {
                assertEquals(VERTEX_VAL, ((List) name).get(0).toString());
            } else {
                fail();
            }
        }
    }
}
