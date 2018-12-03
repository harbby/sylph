package ideal.sylph.plugins.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
* This class is not thread-safe.
* */
public class HbaseHelper {
    private String zk;
    private HTable table;
    private String tableName;
    private String zkNodeParent;
    private static Connection connection;
    private String DEFAULT_FAMLIY = "0";
    public static final String HBASE_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
    public static final String ZOOKEEPER_ZNODE_PARENT = "zookeeper.znode.parent";
    private static Logger logger = LoggerFactory.getLogger(HbaseHelper.class);

    public String getZk() {
        return zk;
    }

    /**
    *
    * */
    public void setZk(String zk) throws IOException {
        this.zk = zk;
    }

    public HbaseHelper() {
    }

    public HbaseHelper(String tableName, String zk, String zkNodeParent) throws IOException {
        this.tableName = tableName;
        this.zk = zk;
        this.zkNodeParent = zkNodeParent;
        initHbaseEnv(zk, zkNodeParent);
    }

    public void initHbaseEnv(String zk, String zkNodeParent) throws IOException {
       if(null == connection){
           synchronized (HbaseHelper.class){
               if(null == connection){
                   Configuration conf = new Configuration();
                   conf.set(HBASE_ZOOKEEPER_QUORUM, zk);
                   if(zkNodeParent != null) conf.set(ZOOKEEPER_ZNODE_PARENT, zkNodeParent);
                   HbaseHelper.connection = ConnectionFactory.createConnection(conf);
                   Runtime.getRuntime().addShutdownHook(new Thread(this::closeConnection));
               }
           }
       }
    }

    /**
     * Is exist get in hbase table.
     * @param get hbase get.
     * @return existence returns true, otherwise returns false.
     * */
    public Boolean existGet(Get get) throws IOException {
        return getTable().exists(get);
    }

    /**
     * Is exist get in hbase table.
     * @param gets a batch of hbase get.
     * @return existence returns true, otherwise returns false.
     * */
    public Boolean[] existGet(List<Get> gets) throws IOException {
        return getTable().exists(gets);
    }

    /**
     * Get result from hbase table.
     * @param get hbase get.
     * @return  Get result.
     * */
    public Result get(Get get) throws IOException {
        return getTable().get(get);
    }

    /**
     * Get result from hbase table.
     * @param gets a batch of hbase get.
     * @return  a batch of Get result.
     * */
    public Result[] get(List<Get> gets) throws IOException {
        return getTable().get(gets);
    }

    public void addColumn(String family, String qualifier, Object columnValue, Put put) {
        put.addColumn(BytesUtil.toBytes(family), BytesUtil.toBytes(qualifier), BytesUtil.toBytes(columnValue));
    }

    public void addColumn(String qualifier, byte[] columnValue, Put put) {
        addColumn(DEFAULT_FAMLIY, qualifier, columnValue, put);
    }

    /**
     * Put data to hbase table.
     * @param put data.
     **/
    public void store(Put put) throws IOException {
        getTable().put(put);
    }

    /**
     * Put data to hbase table.
     * @param puts a baech of data.
     **/
    public void store(List<Put> puts) throws IOException {
        getTable().put(puts);
    }

    /**
     * Put data to hbase table.
     * @param family .
     * @param qualifier .
     * @param timeStamp .
     * @param rowkey .
     * @param value .
     **/
    public void store(byte[] family, byte[] qualifier, Long timeStamp, byte[] rowkey, byte[] value) throws IOException {
        Put put = new Put(rowkey);
        put.addColumn(family, qualifier, timeStamp, value);
        store(put);
    }

    public void store(byte[] family, byte[] qualifier,  byte[] rowkey, byte[] value) throws IOException {
        store(family, qualifier, null, rowkey, value);
    }

    public void flush() throws IOException {
        table.flushCommits();
    }

    /**
     * 刷写数据到hbase
     **/
    public void close() throws IOException {
        table.close();
        table = null;
    }

    /**
     * rollback.
     **/
    public void rollback(){
        logger.warn("This operation is not supported for the time being.");
    }

    /**
     * Is exist hbase table.
     * @param tableName hbase table name.
     * @return existence returns true, otherwise returns false.
     **/
    public boolean tableExist(String tableName) throws IOException {
        Boolean isTableExist = null;
        try (Admin admin = HbaseHelper.connection.getAdmin()) {
            isTableExist = admin.tableExists(TableName.valueOf(tableName));
        }catch (Exception e){
            throw e;
        }
        return isTableExist;
    }

    /**
     * get hbase table connection.
     * @return hbase table conection.
     */
    private HTable getTable(){
        if (tableName == null) return null;
        if (table == null) {
            try {
                table = (HTable) HbaseHelper.connection.getTable(TableName.valueOf(tableName));
                table.setAutoFlush(false, false);
            } catch (Exception e){
                logger.error("get hbase table connection exception. the table is:" + tableName, e);
            }
        }
      return table;
    }

    /**
     * close hbase connection.
     */
    private void closeConnection() {
        try{
            if (null != connection && !connection.isClosed()) {
                connection.close();
                connection = null;
                logger.info("Successful closure of hbase connection.");
            }
        }catch (Exception e){
            logger.error("Close hbase connection exception.", e);
        }
    }
}
