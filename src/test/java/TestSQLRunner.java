import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestSQLRunner {

    private SqlRunner sqlRunner = new SqlRunner();

    private final static String RESULT_SUCCESS = "{\"status\":true,\"errorMsg\":\"\"}";
    private final static String RESULT_FAIL = "{\"status\":false,\"errorMsg\":\"Could not execute CREATE DATABASE: (catalogDatabase: [{}], catalogName: [hive], databaseName: [test_demo_db], ignoreIfExists: [false])\"}";

    @Before
    public void init() {
        System.setProperty("HADOOP_USER_NAME", "tc_warehouse-KYysdaF1");
    }

    //建库
    @Test
    public void testCreateDB() {
        String sql = "create database if not exists " + Conf.FLINK_DB_NAME;
        String result = sqlRunner.executeSQL(sql);

        Assert.assertEquals(RESULT_SUCCESS, result);
    }

    //重复建库
    @Test
    public void testCreateDBTwice() {
        String sql = "create database " + Conf.FLINK_DB_NAME;
        sqlRunner.executeSQL(sql);
        String result = sqlRunner.executeSQL(sql);

        Assert.assertEquals(RESULT_FAIL, result);
    }

    //强制删除库（表也会被删除）
    @Test
    public void testForceDropDB() {
        String sql = "drop database if exists " + Conf.FLINK_DB_NAME + " CASCADE";
        String result = sqlRunner.executeSQL(sql);

        Assert.assertEquals(RESULT_SUCCESS, result);
    }

    //创建 TIDB 映射表
    @Test
    public void testCreateTIDBMappingTable() {
        String sql = "CREATE TABLE " + Conf.FLINK_DB_NAME + ".tidb_stu(" +
                "id int," +
                "name string" +
                ") WITH (" +
                "'connector' = 'tidb'," +
                "'tidb.database.url' = 'jdbc:mysql://" + Conf.TIDB_URL + "/flink '," +
                "'tidb.username' = '" + Conf.TIDB_USER + "'," +
                "'tidb.password' = '" + Conf.TIDB_PASSWROD + "'," +
                "'tidb.database.name' = 'hack_test'," +
                "'tidb.maximum.pool.size' = '10'," +
                "'tidb.minimum.idle.size' = '0'," +
                "'tidb.table.name' = 'tidb_stu'," +
                "'tidb.write_mode' = 'upsert'," +
                "'sink.buffer-flush.max-rows' = '0'" +
                ")";
        String result = sqlRunner.executeSQL(sql);

        Assert.assertEquals(RESULT_SUCCESS, result);
    }

    //创建 TIDB 映射表
    @Test
    public void testCreateTIDBMappingTable1() {
        String sql = "CREATE TABLE " + Conf.FLINK_DB_NAME + ".tidb_stu_details(" +
                "id int," +
                "name string," +
                "city string" +
                ") WITH (" +
                "'connector' = 'tidb'," +
                "'tidb.database.url' = 'jdbc:mysql://" + Conf.TIDB_URL + "/flink'," +
                "'tidb.username' = '" + Conf.TIDB_USER + "'," +
                "'tidb.password' = '" + Conf.TIDB_PASSWROD + "'," +
                "'tidb.database.name' = 'hack_test'," +
                "'tidb.maximum.pool.size' = '10'," +
                "'tidb.minimum.idle.size' = '0'," +
                "'tidb.table.name' = 'tidb_stu'," +
                "'tidb.write_mode' = 'upsert'," +
                "'sink.buffer-flush.max-rows' = '0'" +
                ")";
        String result = sqlRunner.executeSQL(sql);

        Assert.assertEquals(RESULT_SUCCESS, result);
    }

    //删除 TIDB 映射表
    @Test
    public void testDropTIDBMappingTable() {
        String sql = "drop table if exists " + Conf.FLINK_DB_NAME + ".tidb_stu";
        String result = sqlRunner.executeSQL(sql);

        Assert.assertEquals(RESULT_SUCCESS, result);
    }

    //删除 Hive 表
    @Test
    public void testDropHiveTable() {
        String sql = "drop table if exists " + Conf.FLINK_DB_NAME + ".hive_city";
        String result = sqlRunner.executeSQL(sql);

        Assert.assertEquals(RESULT_SUCCESS, result);
    }

    //创建 Hive 表
    @Test
    public void testCreateHiveTable() {
//        String sql = "create table " + FLINK_DB_NAME + ".hive_city(id int, city string) row format delimited fields terminated by ',' stored as TEXTFILE";

        String sql = "CREATE TABLE if not exists " + Conf.FLINK_DB_NAME + ".hive_city(" +
                "  `id` int," +
                "  `city` string)" +
                "ROW FORMAT SERDE" +
                "  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'" +
                "WITH SERDEPROPERTIES (" +
                "  'field.delim'=','," +
                "  'serialization.format'=',')" +
                "STORED AS INPUTFORMAT" +
                "  'org.apache.hadoop.mapred.TextInputFormat'" +
                "OUTPUTFORMAT" +
                "  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'";


        String result = sqlRunner.executeHiveSQL(sql);
        Assert.assertEquals(RESULT_SUCCESS, result);
    }

    //Hive 表插入数据
    @Test
    public void testInsertHiveTable() {
        String[] cities = {"北京", "河北", "天津"};
        for (int i = 0; i < cities.length; i++) {
            //insert into hive_city select 1 as id, '北京' as city;
            String sql = "insert into " + Conf.FLINK_DB_NAME + ".hive_city select " + i + " as id, '" + cities[i] + "' as city";
            sqlRunner.executeSQL(sql);
        }
    }

    //插入
    @Test
    public void testInsert() {
        String sql = "insert into " + "hive.hack_test" + ".tidb_stu_details "
                + " select a.id, a.name, b.city from "
                + Conf.FLINK_DB_NAME + ".tidb_stu as a"
                + " join "
                + Conf.FLINK_DB_NAME + ".hive_city as b"
                + " on a.id=b.id";

        String result = sqlRunner.executeSQL(sql);
        System.out.println(result);
    }

    //Join 联合查询 TIDB Mapping 和 Hive 表
    @Test
    public void testQueryJoinQuery() {
        String sql = "select a.id, a.name, b.city from "
                + Conf.FLINK_DB_NAME + ".`tidb_stu` as a"
                + " join "
                + Conf.FLINK_DB_NAME + ".`hive_city` as b"
                + " on a.id=b.id";

        String result = sqlRunner.executeQuerySQL(sql);

        System.out.println(result);
    }
}