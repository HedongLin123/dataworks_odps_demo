# SpringBoot整合dataworks

## 注意事项

阿里云的dataworks提供了OpenApi, 需要是企业版或旗舰版才能够调用，也就是付费项目。

这里测试主要是调用拉取dataworks上拉取的脚本，并存储到本地。
脚本包含两部分
* 1、开发的odps脚本(通过OpenApi获取)
* 2、建表语句脚本(通过dataworks信息去连接maxCompute获取建立语句)


阿里云Dataworks的openApi分页查询限制，一次最多查询100条。我们拉取脚本需要分多页查询

该项目使用到了MaxCompute的SDK/JDBC方式连接，[SpringBoot操作MaxCompute SDK/JDBC连接]()

## 整合实现
实现主要是编写工具类，如果需要则可以配置成SpringBean，注入容器即可使用

### 依赖引入
```xml
<properties>
    <java.version>1.8</java.version>
    <!--maxCompute-sdk-版本号-->
    <max-compute-sdk.version>0.40.8-public</max-compute-sdk.version>
    <!--maxCompute-jdbc-版本号-->
    <max-compute-jdbc.version>3.0.1</max-compute-jdbc.version>
    <!--dataworks版本号-->
    <dataworks-sdk.version>3.4.2</dataworks-sdk.version>
    <aliyun-java-sdk.version>4.5.20</aliyun-java-sdk.version>
</properties>
<dependencies>
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-web</artifactId>
</dependency>
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-configuration-processor</artifactId>
    <optional>true</optional>
</dependency>
<dependency>
    <groupId>org.projectlombok</groupId>
    <artifactId>lombok</artifactId>
    <optional>true</optional>
</dependency>
<!--max compute sdk-->
<dependency>
    <groupId>com.aliyun.odps</groupId>
    <artifactId>odps-sdk-core</artifactId>
    <version>${max-compute-sdk.version}</version>
</dependency>
<!--max compute jdbc-->
<dependency>
    <groupId>com.aliyun.odps</groupId>
    <artifactId>odps-jdbc</artifactId>
    <version>${max-compute-jdbc.version}</version>
    <classifier>jar-with-dependencies</classifier>
</dependency>
<!--dataworks需要引入aliyun-sdk和dataworks本身-->
<dependency>
    <groupId>com.aliyun</groupId>
    <artifactId>aliyun-java-sdk-core</artifactId>
    <version>${aliyun-java-sdk.version}</version>
</dependency>
<dependency>
    <groupId>com.aliyun</groupId>
    <artifactId>aliyun-java-sdk-dataworks-public</artifactId>
    <version>${dataworks-sdk.version}</version>
</dependency>
</dependencies>
```

### 请求参数类编写
```java
/**
 * @Description
 * @Author itdl
 * @Date 2022/08/09 15:12
 */
@Data
public class DataWorksOpenApiConnParam {
    /**
     * 区域 eg. cn-shanghai
     */
    private String region;

    /**
     * 访问keyId
     */
    private String aliyunAccessId;
    /**
     * 密钥
     */
    private String aliyunAccessKey;

    /**
     * 访问端点  就是API的URL前缀
     */
    private String endPoint;

    /**
     * 数据库类型 如odps
     */
    private String datasourceType;

    /**
     * 所属项目
     */
    private String project;

    /**
     * 项目环境 dev  prod
     */
    private String projectEnv;
}
```


### 工具类编写

#### 基础类准备，拉取脚本之后的回调函数

为什么需要回调函数，因为拉取的是所有脚本，如果合并每次分页结果的话，会导致内存溢出，而使用回调函数只是每次循环增加处理函数

```java
    public static class CallBack {
        public interface FileCallBack {
            /**
             * 每次分页查询文件夹回调
             */
            void handle(List<ListFilesResponse.Data.File> files);
        }


        public interface DdlCallBack {
            /**
             * 获取建表语句回调结果
             */
            void handle(String tableName, String tableDdlContent);
        }
    }
```

#### 初始化操作

主要是实例化dataworks openApi接口的客户端信息，maxCompute连接的工具类初始化(包括JDBC,SDK方式)

```java
private static final String MAX_COMPUTE_JDBC_URL_FORMAT = "http://service.%s.maxcompute.aliyun.com/api";
/**默认的odps接口地址 在Odps中也可以看到该变量*/
private static final String defaultEndpoint = "http://service.odps.aliyun.com/api";
/**
 * dataworks连接参数
 *
 */
private final DataWorksOpenApiConnParam connParam;

/**
 * 可以使用dataworks去连接maxCompute 如果连接的引擎是maxCompute的话
 */
private final MaxComputeJdbcUtil maxComputeJdbcUtil;

private final MaxComputeSdkUtil maxComputeSdkUtil;

private final boolean odpsSdk;


/**
 * 客户端
 */
private final IAcsClient client;

public DataWorksOpenApiUtil(DataWorksOpenApiConnParam connParam, boolean odpsSdk) {
    this.connParam = connParam;
    this.client = buildClient();
    this.odpsSdk = odpsSdk;
    if (odpsSdk){
        this.maxComputeJdbcUtil = null;
        this.maxComputeSdkUtil = buildMaxComputeSdkUtil();
    }else {
        this.maxComputeJdbcUtil = buildMaxComputeJdbcUtil();
        this.maxComputeSdkUtil = null;
    }
}

private MaxComputeSdkUtil buildMaxComputeSdkUtil() {
    final MaxComputeSdkConnParam param = new MaxComputeSdkConnParam();

    // 设置账号密码
    param.setAliyunAccessId(connParam.getAliyunAccessId());
    param.setAliyunAccessKey(connParam.getAliyunAccessKey());

    // 设置endpoint
    param.setMaxComputeEndpoint(defaultEndpoint);

    // 目前只处理odps的引擎
    final String datasourceType = connParam.getDatasourceType();
    if (!"odps".equals(datasourceType)){
        throw new BizException(ResultCode.DATA_WORKS_ENGINE_SUPPORT_ERR);
    }

    // 获取项目环境，根据项目环境连接不同的maxCompute
    final String projectEnv = connParam.getProjectEnv();

    if ("dev".equals(projectEnv)){
        // 开发环境dataworks + _dev就是maxCompute的项目名
        param.setProjectName(String.join("_", connParam.getProject(), projectEnv));
    }else {
        // 生产环境dataworks的项目名和maxCompute一致
        param.setProjectName(connParam.getProject());
    }

    return new MaxComputeSdkUtil(param);
}

private MaxComputeJdbcUtil buildMaxComputeJdbcUtil() {
    final MaxComputeJdbcConnParam param = new MaxComputeJdbcConnParam();

    // 设置账号密码
    param.setAliyunAccessId(connParam.getAliyunAccessId());
    param.setAliyunAccessKey(connParam.getAliyunAccessKey());

    // 设置endpoint
    param.setEndpoint(String.format(MAX_COMPUTE_JDBC_URL_FORMAT, connParam.getRegion()));

    // 目前只处理odps的引擎
    final String datasourceType = connParam.getDatasourceType();
    if (!"odps".equals(datasourceType)){
        throw new BizException(ResultCode.DATA_WORKS_ENGINE_SUPPORT_ERR);
    }

    // 获取项目环境，根据项目环境连接不同的maxCompute
    final String projectEnv = connParam.getProjectEnv();

    if ("dev".equals(projectEnv)){
        // 开发环境dataworks + _dev就是maxCompute的项目名
        param.setProjectName(String.join("_", connParam.getProject(), projectEnv));
    }else {
        // 生产环境dataworks的项目名和maxCompute一致
        param.setProjectName(connParam.getProject());
    }

    return new MaxComputeJdbcUtil(param);
}
```

#### 调用OpenApi拉取所有脚本
```java
/**
 * 根据文件夹路径分页查询该路径下的文件（脚本）
 * @param pageSize 每页查询多少数据
 * @param folderPath 文件所在目录
 * @param userType 文件所属功能模块 可不传
 * @param fileTypes 设置文件代码类型 逗号分割 可不传
 */
public void listAllFiles(Integer pageSize, String folderPath, String userType, String fileTypes, CallBack.FileCallBack callBack) throws ClientException {
    pageSize = setPageSize(pageSize);
    // 创建请求
    final ListFilesRequest request = new ListFilesRequest();

    // 设置分页参数
    request.setPageNumber(1);
    request.setPageSize(pageSize);

    // 设置上级文件夹
    request.setFileFolderPath(folderPath);

    // 设置区域和项目名称
    request.setSysRegionId(connParam.getRegion());
    request.setProjectIdentifier(connParam.getProject());

    // 设置文件所属功能模块
    if (!ObjectUtils.isEmpty(userType)){
        request.setUseType(userType);
    }
    // 设置文件代码类型
    if (!ObjectUtils.isEmpty(fileTypes)){
        request.setFileTypes(fileTypes);
    }

    // 发起请求
    ListFilesResponse res = client.getAcsResponse(request);

    // 获取分页总数
    final Integer totalCount = res.getData().getTotalCount();
    // 返回结果
    final List<ListFilesResponse.Data.File> resultList = res.getData().getFiles();
    // 计算能分几页
    long pages = totalCount % pageSize == 0 ? (totalCount / pageSize) : (totalCount / pageSize) + 1;
    // 只有1页 直接返回
    if (pages <= 1){
        callBack.handle(resultList);
        return;
    }

    // 第一页执行回调
    callBack.handle(resultList);

    // 分页数据 从第二页开始查询 同步拉取，可以优化为多线程拉取
    for (int i = 2; i <= pages; i++) {
        //第1页
        request.setPageNumber(i);
        //每页大小
        request.setPageSize(pageSize);
        // 发起请求
        res = client.getAcsResponse(request);
        final List<ListFilesResponse.Data.File> tableEntityList = res.getData().getFiles();
        if (!ObjectUtils.isEmpty(tableEntityList)){
            // 执行回调函数
            callBack.handle(tableEntityList);
        }
    }
}
```

#### 内部连接MaxCompute拉取所有DDL脚本内容

DataWorks工具类代码，通过回调函数处理

```java
    /**
     * 获取所有的DDL脚本
     * @param callBack 回调处理函数
     */
    public void listAllDdl(CallBack.DdlCallBack callBack){
        if (odpsSdk){
            final List<TableMetaInfo> tableInfos = maxComputeSdkUtil.getTableInfos();
            for (TableMetaInfo tableInfo : tableInfos) {
                final String tableName = tableInfo.getTableName();
                final String sqlCreateDesc = maxComputeSdkUtil.getSqlCreateDesc(tableName);
                callBack.handle(tableName, sqlCreateDesc);
            }
        }
    }
```

MaxCompute工具类代码，根据表名获取建表语句, 以SDK为例， JDBC直接执行show create table即可拿到建表语句

```java
/**
 * 根据表名获取建表语句
 * @param tableName 表名
 * @return
 */
public String getSqlCreateDesc(String tableName) {
    final Table table = odps.tables().get(tableName);
    // 建表语句
    StringBuilder mssqlDDL = new StringBuilder();

    // 获取表结构
    TableSchema tableSchema = table.getSchema();
    // 获取表名表注释
    String tableComment = table.getComment();

    //获取列名列注释
    List<Column> columns = tableSchema.getColumns();
    /*组装成mssql的DDL*/
    // 表名
    mssqlDDL.append("CREATE TABLE IF NOT EXISTS ");
    mssqlDDL.append(tableName).append("\n");
    mssqlDDL.append(" (\n");
    //列字段
    int index = 1;
    for (Column column : columns) {
        mssqlDDL.append("  ").append(column.getName()).append("\t\t").append(column.getTypeInfo().getTypeName());
        if (!ObjectUtils.isEmpty(column.getComment())) {
            mssqlDDL.append(" COMMENT '").append(column.getComment()).append("'");
        }
        if (index == columns.size()) {
            mssqlDDL.append("\n");
        } else {
            mssqlDDL.append(",\n");
        }
        index++;
    }
    mssqlDDL.append(" )\n");
    //获取分区
    List<Column> partitionColumns = tableSchema.getPartitionColumns();
    int partitionIndex = 1;
    if (!ObjectUtils.isEmpty(partitionColumns)) {
        mssqlDDL.append("PARTITIONED BY (");
    }
    for (Column partitionColumn : partitionColumns) {
        final String format = String.format("%s %s COMMENT '%s'", partitionColumn.getName(), partitionColumn.getTypeInfo().getTypeName(), partitionColumn.getComment());
        mssqlDDL.append(format);
        if (partitionIndex == partitionColumns.size()) {
            mssqlDDL.append("\n");
        } else {
            mssqlDDL.append(",\n");
        }
        partitionIndex++;
    }

    if (!ObjectUtils.isEmpty(partitionColumns)) {
        mssqlDDL.append(")\n");
    }
//        mssqlDDL.append("STORED AS ALIORC  \n");
//        mssqlDDL.append("TBLPROPERTIES ('comment'='").append(tableComment).append("');");
    mssqlDDL.append(";");
    return mssqlDDL.toString();
}
```


## 测试代码

```java
public static void main(String[] args) throws ClientException {
    final DataWorksOpenApiConnParam connParam = new DataWorksOpenApiConnParam();
    connParam.setAliyunAccessId("您的阿里云账号accessId");
    connParam.setAliyunAccessKey("您的阿里云账号accessKey");
    // dataworks所在区域
    connParam.setRegion("cn-chengdu");
    // dataworks所属项目
    connParam.setProject("dataworks所属项目");
    // dataworks所属项目环境 如果不分环境的话设置为生产即可
    connParam.setProjectEnv("dev");
    // 数据引擎类型 odps
    connParam.setDatasourceType("odps");
    // ddataworks接口地址
    connParam.setEndPoint("dataworks.cn-chengdu.aliyuncs.com");
    final DataWorksOpenApiUtil dataWorksOpenApiUtil = new DataWorksOpenApiUtil(connParam, true);

    // 拉取所有ODPS脚本
    dataWorksOpenApiUtil.listAllFiles(100, "", "", "10", files -> {
        // 处理文件
        for (ListFilesResponse.Data.File file : files) {
            final String fileName = file.getFileName();
            System.out.println(fileName);
        }
    });

    // 拉取所有表的建表语句
    dataWorksOpenApiUtil.listAllDdl((tableName, tableDdlContent) -> {
        System.out.println("=======================================");
        System.out.println("表名：" + tableName + "内容如下：\n");
        System.out.println(tableDdlContent);
        System.out.println("=======================================");
    });
}
```

## 测试结果

```html
test_001脚本
test_002脚本
test_003脚本
test_004脚本
test_005脚本
=======================================
表名：test_abc_info内容如下：

CREATE TABLE IF NOT EXISTS test_abc_info
 (
    test_abc1		STRING COMMENT '字段1',
    test_abc2		STRING COMMENT '字段2',
    test_abc3		STRING COMMENT '字段3',
    test_abc4		STRING COMMENT '字段4',
    test_abc5		STRING COMMENT '字段5',
    test_abc6		STRING COMMENT '字段6',
    test_abc7		STRING COMMENT '字段7',
    test_abc8		STRING COMMENT '字段8'
 )
PARTITIONED BY (p_date STRING COMMENT '数据日期'
)
;
=======================================
Disconnected from the target VM, address: '127.0.0.1:59509', transport: 'socket'
```
