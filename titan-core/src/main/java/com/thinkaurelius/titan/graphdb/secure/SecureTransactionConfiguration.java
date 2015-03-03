package com.thinkaurelius.titan.graphdb.secure;

import com.thinkaurelius.titan.graphdb.transaction.TransactionConfiguration;

/**
 *
 * @author edeprit@42six.com
 * @param <T>
 */
public interface SecureTransactionConfiguration<T extends SecurityToken> extends TransactionConfiguration {
    
    public boolean hasReadToken();
    
    public boolean hasWriteToken();
    
    public T.ReadToken getReadToken();
    
    public T.WriteToken getWriteToken();
}
