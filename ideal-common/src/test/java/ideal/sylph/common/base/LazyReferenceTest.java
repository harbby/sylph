package ideal.sylph.common.base;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.function.Supplier;

public class LazyReferenceTest
{

    @Test
    public void goLazy()
            throws IOException
    {
        final LazyReference<Connection> connection = LazyReference.goLazy(() -> {
            try {
                return DriverManager.getConnection("jdbc:url");
            }
            catch (SQLException e) {
                throw new RuntimeException("Connection create fail", e);
            }
        });

        Assert.assertNotNull(Serializables.serialize(connection));
    }
}