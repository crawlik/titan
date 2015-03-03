package com.thinkaurelius.titan.graphdb.secure;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.transaction.StandardTransactionBuilder;

/**
 *
 * @author edeprit@42six.com
 * @param <T>
 */
public class SecureTransactionBuilder<T extends SecurityToken> extends StandardTransactionBuilder
    implements SecureTransactionConfiguration {

    private T.ReadToken readToken;

    private T.WriteToken writeToken;

    public SecureTransactionBuilder(GraphDatabaseConfiguration graphConfig, SecureTitanGraph graph) {
        super(graphConfig, graph);
    }

    public SecureTransactionBuilder setReadToken(T.ReadToken readToken) {
        verifyOpen();
        this.readToken = readToken;
        return this;
    }

    public SecureTransactionBuilder setWriteToken(T.WriteToken writeToken) {
        verifyOpen();
        this.writeToken = writeToken;
        return this;
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
    public T.ReadToken getReadToken() {
        return readToken;
    }

    @Override
    public T.WriteToken getWriteToken() {
        return writeToken;
    }
    
        @Override
    public SecureTitanTx<T> start() {
        verifyOpen();
        isOpen = false;
        return ((SecureTitanGraph<T>) graph).newTransaction(this);
    }
}
