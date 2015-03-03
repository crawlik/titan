package com.thinkaurelius.titan.diskstorage.accumulo;

import com.thinkaurelius.titan.diskstorage.common.AbstractStoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTxConfig;
import com.thinkaurelius.titan.graphdb.secure.SecureStoreTxConfig;
import com.thinkaurelius.titan.graphdb.secure.SecurityToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;

/**
 * This creates a transaction type specific to Accumulo.
 *
 * @author Etienne Deprit <edeprit@42six.com>
 */
public class AccumuloStoreTransaction extends AbstractStoreTransaction {

    private static final Authorizations NULL_AUTHORIZATIONS = new Authorizations();
    private static final ColumnVisibility NULL_VISIBILITY = new ColumnVisibility();

    private final Authorizations authorizations;
    private final ColumnVisibility visibility;

    public AccumuloStoreTransaction(final StoreTxConfig config) {
        super(config);

        if (config instanceof SecureStoreTxConfig) {
            SecureStoreTxConfig secureConfig = (SecureStoreTxConfig) config;

            if (secureConfig.hasReadToken()) {
                SecurityToken.ReadToken readToken = secureConfig.getReadToken();
                authorizations = ((AccumuloSecurityToken.ReadToken) readToken).getAuthorizations();
            } else {
                authorizations = NULL_AUTHORIZATIONS;
            }

            if (secureConfig.hasWriteToken()) {
                SecurityToken.WriteToken writeToken = secureConfig.getWriteToken();
                visibility = ((AccumuloSecurityToken.WriteToken) writeToken).getVisibility();

            } else {
                visibility = NULL_VISIBILITY;
            }
        } else {
            authorizations = NULL_AUTHORIZATIONS;
            visibility = NULL_VISIBILITY;
        }
    }

    public Authorizations getAuthorizations() {
        return authorizations;
    }

    public ColumnVisibility getVisibility() {
        return visibility;
    }
}
