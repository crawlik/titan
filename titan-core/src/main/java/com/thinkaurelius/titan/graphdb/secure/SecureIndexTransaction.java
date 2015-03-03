package com.thinkaurelius.titan.graphdb.secure;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.indexing.IndexProvider;
import com.thinkaurelius.titan.diskstorage.indexing.IndexTransaction;
import com.thinkaurelius.titan.diskstorage.indexing.KeyInformation;

/**
 *
 * @author edeprit@42six.com
 * @param <T>
 */
public class SecureIndexTransaction<T extends SecurityToken> extends IndexTransaction implements SecureTransaction<T> {
    
    private T.ReadToken readToken;
    private T.WriteToken writeToken;
    
    public SecureIndexTransaction(final IndexProvider index, final KeyInformation.IndexRetriever keyInformations) throws StorageException {
        super(index, keyInformations);
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
        Preconditions.checkState(readToken != null);
        return readToken;
    }
    
    @Override
    public T.WriteToken getWriteToken() {
        Preconditions.checkState(writeToken != null);
        return writeToken;
    }
    
    @Override
    public final void setReadToken(T.ReadToken readToken) {
        this.readToken = readToken;
        if (indexTx instanceof SecureTransaction) {
            ((SecureTransaction) indexTx).setReadToken(readToken);
        }
    }
    
    @Override
    public final void setWriteToken(T.WriteToken writeToken) {
        this.writeToken = writeToken;
        if (indexTx instanceof SecureTransaction) {
            ((SecureTransaction) indexTx).setWriteToken(writeToken);
        }
    }
}
