package com.thinkaurelius.titan.graphdb.secure;

/**
 *
 * @author edeprit@42six.com
 */
public interface SecurityToken {
    
    public ReadToken newReadToken(Object... parameters);
    
    public WriteToken newWriteToken(Object... parameters);

    public interface ReadToken {

        public boolean subsumes(WriteToken token);

    }

    public interface WriteToken {

        public abstract boolean isNull();

    }
}