package com.thinkaurelius.titan.graphdb.secure;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.ConsistencyLevel;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTxConfig;

/**
 *
 * @author edeprit@42six.com
 * @param <T>
 */
public class SecureStoreTxConfig<T extends SecurityToken> extends StoreTxConfig {

    private T.ReadToken readToken;
    private T.WriteToken writeToken;

    public SecureStoreTxConfig() {
        super();
    }

    public SecureStoreTxConfig(ConsistencyLevel consistency) {
        super(consistency);
    }

    public SecureStoreTxConfig(String metricsPrefix) {
        super(metricsPrefix);
    }

    public SecureStoreTxConfig(ConsistencyLevel consistency, String metricsPrefix) {
        super(consistency, metricsPrefix);
    }

    public boolean hasReadToken() {
        return readToken != null;
    }

    public boolean hasWriteToken() {
        return writeToken != null;
    }

    public T.ReadToken getReadToken() {
        Preconditions.checkState(readToken != null);
        return readToken;
    }

    public T.WriteToken getWriteToken() {
        Preconditions.checkState(writeToken != null);
        return writeToken;
    }

    public StoreTxConfig setReadToken(T.ReadToken readToken) {
        Preconditions.checkArgument(readToken != null);
        this.readToken = readToken;
        return this;
    }

    public StoreTxConfig setWriteToken(T.WriteToken writeToken) {
        Preconditions.checkArgument(writeToken != null);
        this.writeToken = writeToken;
        return this;
    }
}
