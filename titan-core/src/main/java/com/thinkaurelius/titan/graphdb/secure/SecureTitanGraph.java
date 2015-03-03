package com.thinkaurelius.titan.graphdb.secure;

import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.IndexSerializer;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.transaction.StandardTransactionBuilder;
import com.thinkaurelius.titan.graphdb.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author edeprit@42six.com
 * @param <T>
 */
public class SecureTitanGraph<T extends SecurityToken> extends StandardTitanGraph {

    private static final Logger log = LoggerFactory.getLogger(SecureTitanGraph.class);

    public SecureTitanGraph(SecureGraphDatabaseConfiguration configuration) {
        super(configuration);
    }

    @Override
    public SecureTitanTx<T> newTransaction() {
        return buildTransaction().start();
    }

    @Override
    public SecureTransactionBuilder<T> buildTransaction() {
        return new SecureTransactionBuilder<T>(getConfiguration(), this);
    }

    public SecureTitanTx<T> newTransaction(SecureTransactionConfiguration<T> configuration) {
        if (!isOpen) {
            ExceptionFactory.graphShutdown();
        }
        try {
            IndexSerializer.IndexInfoRetriever retriever = indexSerializer.getIndexInforRetriever();
            SecureTitanTx<T> tx = new SecureTitanTx<T>(this, configuration, backend.beginTransaction(configuration, retriever));
            retriever.setTransaction(tx);
            return tx;
        } catch (StorageException e) {
            throw new TitanException("Could not start new transaction", e);
        }
    }
    
    @Override
    public TitanTransaction newThreadBoundTransaction() {
        return new StandardTransactionBuilder(getConfiguration(), this).start();
    }

    @Override
    public String toString() {
        GraphDatabaseConfiguration config = getConfiguration();
        return "securetitangraph" + StringFactory.L_BRACKET
                + config.getBackendDescription() + StringFactory.R_BRACKET;
    }
}
