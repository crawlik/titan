package com.thinkaurelius.titan.graphdb.secure;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.KeyMaker;
import com.thinkaurelius.titan.core.LabelMaker;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.diskstorage.BackendTransaction;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import com.thinkaurelius.titan.graphdb.relations.StandardEdge;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.transaction.TransactionConfiguration;
import com.tinkerpop.blueprints.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author edeprit@42six.com
 * @param <T>
 */
public class SecureTitanTx<T extends SecurityToken> extends StandardTitanTx {

    private static final Logger log = LoggerFactory.getLogger(SecureTitanTx.class);

    public SecureTitanTx(StandardTitanGraph graph, TransactionConfiguration config, BackendTransaction txHandle) {
        super(graph, config, txHandle);
    }

    @Override
    public KeyMaker makeKey(String name) {
        SecureTransactionConfiguration config = (SecureTransactionConfiguration) getConfiguration();
        Preconditions.checkState(!config.hasWriteToken() || config.getWriteToken().isNull(),
                "Cannot define key with non-null write token");

        SecureKeyMaker maker = new SecureKeyMaker(this, indexSerializer);
        maker.name(name);
        return maker;
    }

    @Override
    public LabelMaker makeLabel(String name) {
        SecureTransactionConfiguration config = (SecureTransactionConfiguration) getConfiguration();
        Preconditions.checkState(!config.hasWriteToken() || config.getWriteToken().isNull(),
                "Cannot define label with non-null write token");

        SecureLabelMaker maker = new SecureLabelMaker(this, indexSerializer);
        maker.name(name);
        return maker;
    }

    @Override
    public TitanVertex addVertex(Long vertexId) {
        SecureTransactionConfiguration config = (SecureTransactionConfiguration) getConfiguration();
        Preconditions.checkState(!config.hasWriteToken() || config.getWriteToken().isNull(),
                "Cannot add vertex with non-null write token");
        return super.addVertex(vertexId);
    }

    @Override
    public void removeVertex(Vertex vertex) {
        throw new TitanException("Cannot remove vertex: " + vertex);    
    }

    @Override
    public synchronized void commit() {
        for (InternalRelation relation : addedRelations.getAll()) {
            if (relation instanceof StandardEdge) {
                StandardEdge edge = (StandardEdge) relation;
                if (edge.getPreviousID() > 0) {
                    throw new TitanException("Cannot modify immutable edge: " + edge);
                }
            }
        }
        super.commit();
    }
}
