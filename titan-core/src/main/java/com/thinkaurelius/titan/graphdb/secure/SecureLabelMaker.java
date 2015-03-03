package com.thinkaurelius.titan.graphdb.secure;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.LabelMaker;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.graphdb.database.IndexSerializer;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.StandardLabelMaker;
import com.tinkerpop.blueprints.Direction;

/**
 *
 * @author edeprit@42six.com
 */
public class SecureLabelMaker extends StandardLabelMaker {

    public SecureLabelMaker(StandardTitanTx tx, IndexSerializer indexSerializer) {
        super(tx, indexSerializer);
    }

    @Override
    public TitanLabel make() {
        Preconditions.checkArgument(!(isUnique(Direction.IN) || isUnique(Direction.OUT)), "Edges may only be many-to-many.");
        return super.make();
    }
}
