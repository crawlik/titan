package com.thinkaurelius.titan.graphdb.accumulo;

import com.thinkaurelius.titan.AccumuloStorageSetup;
import com.thinkaurelius.titan.diskstorage.accumulo.AccumuloSecurityToken;
import com.thinkaurelius.titan.graphdb.SecureTitanGraphTest;
import com.thinkaurelius.titan.graphdb.secure.SecureTitanGraph;
import com.thinkaurelius.titan.graphdb.secure.SecureTransactionBuilder;
import com.thinkaurelius.titan.graphdb.secure.SecurityToken;
import java.io.IOException;
import org.junit.BeforeClass;

/**
 *
 * @author edeprit@42six.com
 */
public class AccumuloSecureGraphTest extends SecureTitanGraphTest {

    public AccumuloSecureGraphTest() {
        super(AccumuloStorageSetup.getAccumuloGraphConfiguration());
    }

    @BeforeClass
    public static void startAccumulo() throws IOException {
        AccumuloStorageSetup.startAccumulo();
    }

    @Override
    public void startReadTx(ReadToken token) {
        AccumuloSecurityToken.ReadToken art = AccumuloSecurityToken.getInstance().newReadToken(token.toString());
        tx = secureTransactionBuilder()
                .setReadToken(art)
                .start();
    }

    @Override
    public void startWriteTx(WriteToken token) {
        AccumuloSecurityToken.WriteToken awt = AccumuloSecurityToken.getInstance().newWriteToken(token.toString());
        tx = secureTransactionBuilder()
                .setWriteToken(awt)
                .start();
    }

    @Override
    public void startReadWriteTx(ReadToken readToken, WriteToken writeToken) {
        AccumuloSecurityToken factory = AccumuloSecurityToken.getInstance();
        AccumuloSecurityToken.ReadToken art = factory.newReadToken(readToken.toString());
        AccumuloSecurityToken.WriteToken awt = factory.newWriteToken(writeToken.toString());
        tx = secureTransactionBuilder()
                .setReadToken(art)
                .setWriteToken(awt)
                .start();
    }

    private SecureTransactionBuilder secureTransactionBuilder() {
        return ((SecureTitanGraph<SecurityToken>) graph).buildTransaction();
    }
}
