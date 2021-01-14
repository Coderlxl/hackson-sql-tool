# hackson-sql-tool

####本地调试 <br>
1.完善 Conf 保存了 TIDB 集群的连接信息 

    //例如：10.168.10.2:4000
    public final static String TIDB_URL = "${ip}:${port}";

    //例如：admin
    public final static String TIDB_USER = "${user}";

    //例如：123456
    public final static String TIDB_PASSWROD = "${password}";
    
    //例如：
    //注意：本地开发的时候，可以设置 HIVE_CONF_DIR = "src/main/resources/hive/"; 并把 hive conf 文件放到 resources/hive/目录下
    public static String HIVE_CONF_DIR = "";