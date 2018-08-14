package ideal.sylph.plugins.flink.sink;

import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.etl.Row;
import ideal.sylph.etl.api.RealTimeSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import static java.util.Objects.requireNonNull;

@Name("mysql")
@Description("this is mysql Sink, if table not execit ze create table")
public class MysqlSink
        implements RealTimeSink
{
    private static final Logger logger = LoggerFactory.getLogger(MysqlSink.class);

    private String url;
    private String userName;
    private String password;

    private Connection connection;
    private PreparedStatement statement;
    private int num = 0;

    @Override
    public void driverInit(Map<String, Object> optionMap)
    {
        this.url = (String) requireNonNull(optionMap.get("url"), "url is not setting");
        this.userName = (String) requireNonNull(optionMap.get("userName"), "userName is not setting");
        this.password = (String) requireNonNull(optionMap.get("password"), "password is not setting");
    }

    @Override
    public boolean open(long partitionId, long version)
    {
        String sql = "insert into mysql_table_sink values(?,?,?)";
        try {
            Class.forName("com.mysql.jdbc.Driver");
            this.connection = DriverManager.getConnection(url, userName, password);
            this.statement = connection.prepareStatement(sql);
        }
        catch (SQLException | ClassNotFoundException e) {
            throw new RuntimeException("MysqlSink open fail", e);
        }
        return true;
    }

    @Override
    public void process(Row value)
    {
        try {
            for (int i = 0; i < value.size(); i++) {
                statement.setObject(i + 1, value.getAs(i));
            }
            statement.addBatch();
            // submit batch
            if (num >= 5) {
                statement.executeBatch();
                num = 0;
            }
            else {
                num++;
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close(Throwable errorOrNull)
    {
        try (Connection conn = connection) {
            try (Statement stmt = statement) {
                if (stmt != null) {
                    stmt.executeBatch();
                }
            }
            catch (SQLException e) {
                logger.error("close executeBatch fail", e);
            }
        }
        catch (SQLException e) {
            logger.error("close connection fail", e);
        }
    }
}
