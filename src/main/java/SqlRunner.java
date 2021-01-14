import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.util.ArrayList;

/**
 * 执行 SQL 语句
 */
public class SqlRunner {

    private static EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();

    public String executeSQL(String sql) {
        return executeSQL(false, sql);
    }

    public String executeHiveSQL(String sql) {
        return executeSQL(true, sql);
    }

    /**
     * @param sql: DDL / Insert SQL
     * @return
     */
    private String executeSQL(boolean isHiveSQL, String sql) {
        TableEnvironment tableEnvironment = TableEnvironment.create(settings);
        HiveCatalog hiveCatalog = new HiveCatalog("hive", "default", Conf.HIVE_CONF_DIR);
        tableEnvironment.registerCatalog("hive", hiveCatalog);

        // 使用 Hive dialect
        if (isHiveSQL) {
            tableEnvironment.getConfig().setSqlDialect(SqlDialect.HIVE);
        }

        boolean success = false;
        String errorMsg = "";
        try {
            TableResult tableResult = tableEnvironment.executeSql(sql);
            tableResult.print();
            success = true;
        } catch (Exception e) {
            errorMsg = e.getMessage();
        } finally {
            return OP.opStatus(success, errorMsg);
        }
    }

    /**
     * @param sql: Select SQL
     * @return
     */
    public String executeQuerySQL(String sql) {
        TableEnvironment tableEnvironment = TableEnvironment.create(settings);
        HiveCatalog hiveCatalog = new HiveCatalog("hive", "default", Conf.HIVE_CONF_DIR);
        tableEnvironment.registerCatalog("hive", hiveCatalog);

        String errorMsg = "";
        ArrayList<String[]> list = new ArrayList<>();
        try {
            TableResult tableResult = tableEnvironment.executeSql(sql);
            try (CloseableIterator<Row> it = tableResult.collect()) {
                while (it.hasNext()) {
                    Row row = it.next();
                    list.add(row.toString().split(","));
                }
            }
        } catch (Exception e) {
            errorMsg = e.getMessage();
        } finally {
            return OP.opResult(list, errorMsg);
        }
    }
}
