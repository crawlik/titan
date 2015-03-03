package com.thinkaurelius.titan.diskstorage.indexing;

import com.thinkaurelius.titan.diskstorage.StorageException;
import static com.thinkaurelius.titan.diskstorage.indexing.IndexProviderTest.indexRetriever;
import com.thinkaurelius.titan.graphdb.secure.SecureIndexTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class SecureIndexProviderTest extends IndexProviderTest {

    private final Logger log = LoggerFactory.getLogger(SecureIndexProviderTest.class);

    @Override
    public void open() throws StorageException {
        index = openIndex();
        tx = new SecureIndexTransaction(index, indexRetriever);
    }
}
