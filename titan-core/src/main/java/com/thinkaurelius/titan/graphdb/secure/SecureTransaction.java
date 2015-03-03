package com.thinkaurelius.titan.graphdb.secure;

/**
 *
 * @author edeprit@42six.com
 * @param <T>
 */
public interface SecureTransaction<T extends SecurityToken> {
    
    public boolean hasReadToken();
    
    public boolean hasWriteToken();
    
    public T.ReadToken getReadToken();

    public T.WriteToken getWriteToken();
    
    public void setReadToken(T.ReadToken readToken);
    
    public void setWriteToken(T.WriteToken writeToken);
}
