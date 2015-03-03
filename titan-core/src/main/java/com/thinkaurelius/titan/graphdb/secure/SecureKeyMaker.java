package com.thinkaurelius.titan.graphdb.secure;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.KeyMaker;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.graphdb.database.IndexSerializer;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.StandardKeyMaker;
import com.tinkerpop.blueprints.Direction;

/**
 *
 * @author edeprit@42six.com
 */
public class SecureKeyMaker extends StandardKeyMaker {

    public SecureKeyMaker(StandardTitanTx tx, IndexSerializer indexSerializer) {
        super(tx, indexSerializer);
        // list();
    }

    @Override
    public TitanKey make() {
        // Preconditions.checkArgument(!this.isUnique(Direction.IN), "Unique constraint not supported.");
        return super.make();
    }
}
