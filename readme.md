# SpringBoot集成MaxCompute

## 1、SDK方式集成

使用odps-sdk-core集成, 官方文档地址[MaxCompute Java SDK介绍](https://help.aliyun.com/document_detail/34614.html)

### 1.1、依赖引入odps-sdk-core

  
```xml
<properties>
    <java.version>1.8</java.version>
    <!--maxCompute sdk 版本号-->
    <max-compute-sdk.version>0.40.8-public</max-compute-sdk.version>
</properties>

<dependencies>
  <!--max compute sdk-->
  <dependency>
    <groupId>com.aliyun.odps</groupId>
    <artifactId>odps-sdk-core</artifactId>
    <version>${max-compute-sdk.version}</version>
</dependency>
</dependencies>
```
  
### 1.2、编写连接工具类

编写MaxComputeSdkUtil以SDK方式连接MaxCompute

#### 1.2.1、重要类和方法说明

* 1、连接参数类：
  
    ```java
    @Data
    public class MaxComputeSdkConnParam {
        /**阿里云accessId 相当于用户名 */
        private String aliyunAccessId;
        /**阿里云accessKey 相当于密码 */
        private String aliyunAccessKey;
        /**阿里云maxCompute服务接口地址 默认是http://service.odps.aliyun.com/api*/
        private String maxComputeEndpoint;
        /**项目名称*/
        private String projectName;
    }
    ```
  

* 2、查询表元数据信息实体

主要是字段：tableName, comment。还可以自己添加其他字段

```java
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableMetaInfo {
    /**表名称*/
    private String tableName;
    /**表注释*/
    private String comment;
}

```

* 3、公共方法(初始化)

```java
    /**默认的odps接口地址 在Odps中也可以看到该变量*/
    private static final String defaultEndpoint = "http://service.odps.aliyun.com/api";
    /**开启全表扫描的配置*/
    private static final String FULL_SCAN_CONFIG = "odps.sql.allow.fullscan";
    /**分页查询sql模板*/
    private static final String PAGE_SELECT_TEMPLATE_SQL = "select z.* from (%s) z limit %s, %s;";
    /**分页查询统计数量模板SQL*/
    private static final String PAGE_COUNT_TEMPLATE_SQL = "select count(1) from (%s) z;";
    /**sdk的odps客户端*/
    private final Odps odps;
    
    /**odps连接参数*/
    private final MaxComputeSdkConnParam connParam;
    
    public MaxComputeSdkUtil(MaxComputeSdkConnParam param){
        this.connParam = param;
        // 构建odps客户端
        this.odps = buildOdps();
    }
    
    /**
     * 构建odps客户端 用于执行sql等操作
     * @return odps客户端
     */
    private Odps buildOdps() {
        // 阿里云账号密码  AccessId 和 AccessKey
        final String aliyunAccessId = connParam.getAliyunAccessId();
        final String aliyunAccessKey = connParam.getAliyunAccessKey();
        // 创建阿里云账户
        final AliyunAccount aliyunAccount = new AliyunAccount(aliyunAccessId, aliyunAccessKey);
    
        // 使用阿里云账户创建odps客户端
        final Odps odps = new Odps(aliyunAccount);
    
        // 传入了的话就是用传入的 没有传入使用默认的
        final String endpoint = connParam.getMaxComputeEndpoint();
        try {
            odps.setEndpoint(ObjectUtils.isEmpty(endpoint) ? defaultEndpoint : endpoint);
        } catch (Exception e) {
            // 端点格式不正确
            throw new BizException(ResultCode.MAX_COMPUTE_ENDPOINT_ERR);
        }
    
        // 设置项目
        odps.setDefaultProject(connParam.getProjectName());
        return odps;
    }
```

* 4、查询表信息
```java
    /**
     * 获取表信息
     */
    public List<TableMetaInfo> getTableInfos(){
        final Tables tables = odps.tables();
        List<TableMetaInfo> resultTables = new ArrayList<>();
        try {
            for (Table table : tables) {
                // tableName
                final String name = table.getName();
                // 描述
                final String comment = table.getComment();
                final TableMetaInfo info = new TableMetaInfo(name, comment);
                resultTables.add(info);
            }
        } catch (Exception e) {
            e.printStackTrace();
            final String errMsg = ObjectUtils.isEmpty(e.getMessage()) ? "" : e.getMessage();
            if (errMsg.contains("ODPS-0410051:Invalid credentials")){
                throw new BizException(ResultCode.MAX_COMPUTE_UNAME_ERR);
            }
            if (errMsg.contains("ODPS-0410042:Invalid signature value")){
                throw new BizException(ResultCode.MAX_COMPUTE_PWD_ERR);
            }
            if (errMsg.contains("ODPS-0420095: Access Denied")){
                throw new BizException(ResultCode.MAX_COMPUTE_PROJECT_ERR);
            }
        }
        return resultTables;
    }

```

* 5、执行SQL封装
```java
    /**
     * 执行sql查询
     * @param querySql 查询sql
     * @param fullScan 是否开启全表扫描 如果查询多个分区数据，需要开启全表扫描
     * @return List<Map<String, Object>>
     */
    public List<Map<String, Object>> queryData(String querySql, boolean fullScan){
        try {
            // 配置全表扫描吗
            configFullScan(fullScan);
            // 使用任务执行SQL
            final Instance instance = SQLTask.run(odps, querySql);
            // 等待执行成功
            instance.waitForSuccess();
            // 封装返回结果
            List<Record> records = SQLTask.getResult(instance);
            // 结果转换为Map
            return buildMapByRecords(records);
        } catch (OdpsException e) {
            e.printStackTrace();
            throw new BizException(ResultCode.MAX_COMPUTE_SQL_EXEC_ERR);
        }
    }

    /**
     * 开启和移除全表扫描配置
     * @param fullScan 是否全表扫描
     */
    private void configFullScan(boolean fullScan) {
            if (fullScan){
            // 开启全表扫描配置
            Map<String, String> config = new HashMap<>();
            log.info("===>>开启全表扫描， 查询多个分区数据");
            config.put(FULL_SCAN_CONFIG, "true");
            odps.setGlobalSettings(config);
            }else {
            // 移除全表扫描配置
            odps.getGlobalSettings().remove(FULL_SCAN_CONFIG);
            }
        }

    /**
     * 将List<Record>准换为List<Map></>
     * @param records sql查询结果
     * @return 返回结果
     */
    private List<Map<String, Object>> buildMapByRecords(List<Record> records) {
            List<Map<String, Object>> listMap = new ArrayList<>();
            for (Record record : records) {
            Column[] columns = record.getColumns();
            Map<String, Object> map = new LinkedHashMap<>();
            for (Column column : columns) {
            String name = column.getName();
            Object value = record.get(name);
            // maxCompute里面的空返回的是使用\n
            if ("\\N".equalsIgnoreCase(String.valueOf(value))) {
            map.put(name, "");
            } else {
            map.put(name, value);
            }
            }
            listMap.add(map);
            }
            return listMap;
        }
```

* 6、分页查询分装
```java
    /**
     * 执行sql查询【分页查询】
     * @param querySql 查询sql
     * @param page 页码 从1开始 第n页传n
     * @param size 每页记录数
     * @param fullScan 是否开启全表扫描 如果查询多个分区数据，需要开启全表扫描
     * @return List<Map<String, Object>>
     */
    public List<Map<String, Object>> queryData(String querySql, Integer page, Integer size, boolean fullScan){
        // 重写SQl，添加limit offset, limit
        // 1、替换分号
        querySql = querySql.replaceAll(";", "");
        // 2、格式化SQL
        Integer offset = (page - 1 ) * size;
        // 得到执行sql
        final String execSql = String.format(PAGE_SELECT_TEMPLATE_SQL, querySql, offset, size);
        log.info("=======>>>执行分页sql为：{}", execSql);

        // 调用执行SQL数据
        return queryData(execSql, fullScan);
    }


    /**
     * 执行分页查询
     * @param querySql 分页查询sql
     * @param page 页码 从1开始 第n页传n
     * @param size 每页记录数
     * @return 分页查询结果
     */
    public PageResult<Map<String, Object>> pageQueryMap(String querySql, Integer page, Integer size){
        // 1、替换分号
        querySql = querySql.replaceAll(";", "");
        String countSql = String.format(PAGE_COUNT_TEMPLATE_SQL, querySql);
        log.info("=======>>>执行分页统计总数sql为：{}", countSql);
        // 查询总数
        final List<Map<String, Object>> countMap = queryData(countSql, false);
        if (CollectionUtils.isEmpty(countMap)){
            return new PageResult<>(0L, new ArrayList<>());
        }

        long count = 0L;
        for (Object value : countMap.get(0).values()) {
            count = Long.parseLong(String.valueOf(value));
        }

        if (count == 0){
            return new PageResult<>(0L, new ArrayList<>());
        }

        // 执行分页查询 开启全表扫描
        final List<Map<String, Object>> resultList = queryData(querySql, page, size, true);

        return new PageResult<>(count, resultList);
    }


    /**
     * 执行分页查询
     * @param querySql 分页查询sql
     * @param page 页码 从1开始 第n页传n
     * @param size 每页记录数
     * @return 分页查询结果
     */
    public <T>PageResult<T> pageQuery(String querySql, Integer page, Integer size, Class<T> clazz){
        final PageResult<Map<String, Object>> result = pageQueryMap(querySql, page, size);
        List<T> rows = new ArrayList<>();
        for (Map<String, Object> row : result.getRows()) {
            final T t = JSONObject.parseObject(JSONObject.toJSONString(row), clazz);
            rows.add(t);
        }
        return new PageResult<>(result.getTotal(), rows);
    }
```

#### 1.2.2 工具类测试

使用测试数据测试工具类

```java
public static void main(String[] args) {
    // 构建连接参数
    final MaxComputeSdkConnParam connParam = new MaxComputeSdkConnParam();
    connParam.setAliyunAccessId("您的阿里云账号accessId");
    connParam.setAliyunAccessKey("您的阿里云账号accessKey");
    connParam.setProjectName("项目名");

    // 实例化工具类
    final MaxComputeSdkUtil sdkUtil = new MaxComputeSdkUtil(connParam);

    // 查询所有表
    final List<TableMetaInfo> tableInfos = sdkUtil.getTableInfos();
    for (TableMetaInfo tableInfo : tableInfos) {
        System.out.println(tableInfo.getTableName());
    }

    // 分页查询数据
    final PageResult<Map<String, Object>> page = sdkUtil.pageQueryMap("select * from ods_cust;", 2, 10);
    System.out.println(page.getTotal());
    for (Map<String, Object> map : page.getRows()) {
        System.out.println(JSONObject.toJSONString(map));
    }
}
```

#### 1.2.3 为什么要开启全表扫描

maxCompute存在使用限制如下：

当使用select语句时，屏显最多只能显示10000行结果。当select语句作为子句时则无此限制，select子句会将全部结果返回给上层查询。
select语句查询分区表时默认禁止全表扫描。
自2018年1月10日20:00:00后，在新创建的项目上执行SQL语句时，默认情况下，针对该项目里的分区表不允许执行全表扫描操作。在查询分区表数据时必须指定分区，由此减少SQL的不必要I/O，从而减少计算资源的浪费以及按量计费模式下不必要的计算费用。

如果您需要对分区表进行全表扫描，可以在全表扫描的SQL语句前加上命令set odps.sql.allow.fullscan=true;，并和SQL语句一起提交执行。假设sale_detail表为分区表，需要同时执行如下语句进行全表查询：


## 2、JDBC方式集成

使用odps-jdbc集成, 官方文档地址[MaxCompute Java JDBC介绍](https://help.aliyun.com/document_detail/161246.html)


```xml
<properties>
    <java.version>1.8</java.version>
    <!--maxCompute-jdbc-版本号-->
    <max-compute-jdbc.version>3.0.1</max-compute-jdbc.version>
</properties>

<dependencies>
  <!--max compute jdbc-->
  <dependency>
    <groupId>com.aliyun.odps</groupId>
    <artifactId>odps-jdbc</artifactId>
    <version>${max-compute-jdbc.version}</version>
    <classifier>jar-with-dependencies</classifier>
  </dependency>
</dependencies>
```

### 2.2、编写连接工具类

编写MaxComputeSdkUtil以JDBC方式连接MaxCompute

#### 2.2.1、重要类和方法说明

* 1、连接参数类：

```java
@Data
public class MaxComputeJdbcConnParam {
  /**阿里云accessId 相当于用户名 */
  private String aliyunAccessId;
  /**阿里云accessKey 相当于密码 */
  private String aliyunAccessKey;
  /** maxcompute_endpoint */
  private String endpoint;
  /**项目名称*/
  private String projectName;
}
```

* 2、公共方法(初始化)

```java
    /**JDBC 驱动名称*/
    private static final String DRIVER_NAME = "com.aliyun.odps.jdbc.OdpsDriver";

    private static final String SELECT_ALL_TABLE_SQL = "select table_name, table_comment from Information_Schema.TABLES";

    private static final String SELECT_FIELD_BY_TABLE_SQL = "select column_name, column_comment from Information_Schema.COLUMNS where table_name = '%s'";
    /**分页查询sql模板*/
    private static final String PAGE_SELECT_TEMPLATE_SQL = "select z.* from (%s) z limit %s, %s;";
    /**分页查询统计数量模板SQL*/
    private static final String PAGE_COUNT_TEMPLATE_SQL = "select count(1) from (%s) z;";
    /**连接*/
    private final Connection conn;

    /**
     * 连接参数
     */
    private final MaxComputeJdbcConnParam connParam;

    public MaxComputeJdbcUtil(MaxComputeJdbcConnParam connParam) {
        this.connParam = connParam;
        this.conn = buildConn();
    }

    /**
     * 创建连接
     * @return 数据库连接
     */
    private Connection buildConn() {
        try {
            Class.forName(DRIVER_NAME);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new BizException(ResultCode.MAX_COMPUTE_JDBC_DRIVE_LOAD_ERR);
        }

        try {
            // JDBCURL连接模板
            String jdbcUrlTemplate = "jdbc:odps:%s?project=%s&useProjectTimeZone=true";
            // 使用驱动管理器连接获取连接
            return DriverManager.getConnection(
                    String.format(jdbcUrlTemplate, connParam.getEndpoint(), connParam.getProjectName()),
                    connParam.getAliyunAccessId(), connParam.getAliyunAccessKey());
        } catch (SQLException e) {
            e.printStackTrace();
            throw new BizException(ResultCode.MAX_COMPUTE_JDBC_DRIVE_LOAD_ERR);
        }
    }
```

* 3、查询表信息
```java
    /**
     * 获取表信息
     * @return 表信息列表
     */
    public List<TableMetaInfo> getTableInfos(){
        List<TableMetaInfo> resultList = new ArrayList<>();
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            // 创建statement 使用SQL直接查询
            statement = conn.createStatement();
            // 执行查询语句
            resultSet = statement.executeQuery(SELECT_ALL_TABLE_SQL);
            while (resultSet.next()){
                final String tableName = resultSet.getString("table_name");
                final String tableComment = resultSet.getString("table_comment");
                final TableMetaInfo info = new TableMetaInfo(tableName, tableComment);
                resultList.add(info);
            }

            return resultList;
        } catch (SQLException e) {
            e.printStackTrace();
            throw new BizException(ResultCode.MAX_COMPUTE_SQL_EXEC_ERR);
        } finally {
            // 关闭resultSet
            closeResultSet(resultSet);
            // 关闭statement
            closeStatement(statement);
        }
    }
```

* 4、执行SQL封装
```java
    /**
     * 执行sql查询
     * @param querySql 查询sql
     * @return List<Map<String, Object>>
     */
    public List<Map<String, Object>> queryData(String querySql){
        List<Map<String, Object>> resultList = new ArrayList<>();
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            // 创建statement
            statement = conn.createStatement();

            // 执行查询语句
            resultSet = statement.executeQuery(querySql);

            // 构建结果返回
            buildMapByRs(resultList, resultSet);

            return resultList;
        } catch (SQLException e) {
            e.printStackTrace();
            throw new BizException(ResultCode.MAX_COMPUTE_SQL_EXEC_ERR);
        } finally {
            // 关闭resultSet
            closeResultSet(resultSet);
            // 关闭statement
            closeStatement(statement);
        }
    }

    /**
     * 将ResultSet转换为List<Map<String, Object>>
     * @param resultList 转换的集合
     * @param resultSet ResultSet
     * @throws SQLException e
     */
    private void buildMapByRs(List<Map<String, Object>> resultList, ResultSet resultSet) throws SQLException {
        // 获取元数据
        ResultSetMetaData metaData = resultSet.getMetaData();
        while (resultSet.next()) {
            // 获取列数
            int columnCount = metaData.getColumnCount();
            Map<String, Object> map = new HashMap<>();
            for (int i = 0; i < columnCount; i++) {
                String columnName = metaData.getColumnName(i + 1);
                Object object = resultSet.getObject(columnName);
                // maxCompute里面的空返回的是使用\n
                if ("\\N".equalsIgnoreCase(String.valueOf(object))) {
                    map.put(columnName, "");
                } else {
                    map.put(columnName, object);
                }
            }
            resultList.add(map);
        }
    }


    private void closeStatement(Statement statement){
        if (statement != null){
            try {
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private void closeResultSet(ResultSet resultSet){
        if (resultSet != null){
            try {
                resultSet.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
```

* 5、分页查询分装
```java
   /**
     * 执行sql查询
     * @param querySql 查询sql
     * @return List<Map<String, Object>>
     */
    public List<Map<String, Object>> queryData(String querySql, Integer page, Integer size){
        List<Map<String, Object>> resultList = new ArrayList<>();
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            // 1、替换分号
            querySql = querySql.replaceAll(";", "");
            // 创建statement
            statement = conn.createStatement();
            // 2、格式化SQL
            int offset = (page - 1 ) * size;
            final String execSql = String.format(PAGE_SELECT_TEMPLATE_SQL, querySql, offset, size);
            log.info("=======>>>执行分页sql为：{}", execSql);
            // 执行查询语句
            resultSet = statement.executeQuery(execSql);

            // 构建结果返回
            buildMapByRs(resultList, resultSet);
            return resultList;
        } catch (SQLException e) {
            e.printStackTrace();
            throw new BizException(ResultCode.MAX_COMPUTE_SQL_EXEC_ERR);
        } finally {
            // 关闭resultSet
            closeResultSet(resultSet);
            // 关闭statement
            closeStatement(statement);
        }
    }


    /**
     * 执行分页查询
     * @param querySql 分页查询sql
     * @param page 页码 从1开始 第n页传n
     * @param size 每页记录数
     * @return 分页查询结果
     */
    public PageResult<Map<String, Object>> pageQueryMap(String querySql, Integer page, Integer size){
        // 1、替换分号
        querySql = querySql.replaceAll(";", "");
        String countSql = String.format(PAGE_COUNT_TEMPLATE_SQL, querySql);
        log.info("=======>>>执行分页统计总数sql为：{}", countSql);
        // 查询总数
        final List<Map<String, Object>> countMap = queryData(countSql);
        if (CollectionUtils.isEmpty(countMap)){
            return new PageResult<>(0L, new ArrayList<>());
        }

        long count = 0L;
        for (Object value : countMap.get(0).values()) {
            count = Long.parseLong(String.valueOf(value));
        }

        if (count == 0){
            return new PageResult<>(0L, new ArrayList<>());
        }

        // 执行分页查询 开启全表扫描
        final List<Map<String, Object>> resultList = queryData(querySql, page, size);

        return new PageResult<>(count, resultList);
    }


    /**
     * 执行分页查询
     * @param querySql 分页查询sql
     * @param page 页码 从1开始 第n页传n
     * @param size 每页记录数
     * @return 分页查询结果
     */
    public <T>PageResult<T> pageQuery(String querySql, Integer page, Integer size, Class<T> clazz){
        final PageResult<Map<String, Object>> result = pageQueryMap(querySql, page, size);
        List<T> rows = new ArrayList<>();
        for (Map<String, Object> row : result.getRows()) {
            final T t = JSONObject.parseObject(JSONObject.toJSONString(row), clazz);
            rows.add(t);
        }
        return new PageResult<>(result.getTotal(), rows);
    }
```

#### 2.2.2 工具类测试

使用测试数据测试工具类

```java
    public static void main(String[] args) {
        final MaxComputeJdbcConnParam connParam = new MaxComputeJdbcConnParam();
        connParam.setAliyunAccessId("您的阿里云账号accessId");
        connParam.setAliyunAccessKey("您的阿里云账号accessKey");
        connParam.setProjectName("项目名");
        connParam.setEndpoint("http://service.cn-hangzhou.maxcompute.aliyun.com/api");
        final MaxComputeJdbcUtil jdbcUtil = new MaxComputeJdbcUtil(connParam);

        // 获取表信息
        final List<TableMetaInfo> tableInfos = jdbcUtil.getTableInfos();
        for (TableMetaInfo tableInfo : tableInfos) {
            System.out.println(tableInfo);
        }

        // 获取字段信息
        final String tableName = tableInfos.get(new Random().nextInt(tableInfos.size())).getTableName();
        final List<TableColumnMetaInfo> fields = jdbcUtil.getFieldByTableName(tableName);
        for (TableColumnMetaInfo field : fields) {
            System.out.println(field.getFieldName() + "-" + field.getComment());
        }

        // 执行查询
        final List<Map<String, Object>> list = jdbcUtil.queryData("select * from ods_cust;");
        for (Map<String, Object> map : list) {
            System.out.println(JSONObject.toJSONString(map));
        }

        // 执行分页查询
        final List<Map<String, Object>> list2 = jdbcUtil.queryData("select * from ods_cust;", 2, 10);
        for (Map<String, Object> map : list2) {
            System.out.println(JSONObject.toJSONString(map));
        }

        // 执行分页查询 并返回count
        final PageResult<Map<String, Object>> list3 = jdbcUtil.pageQueryMap("select * from ods_cust;", 2, 10);
        System.out.println(list3.getTotal());
        for (Map<String, Object> map : list3.getRows()) {
            System.out.println(JSONObject.toJSONString(map));
        }

        jdbcUtil.close();
    }
```


## 项目地址








