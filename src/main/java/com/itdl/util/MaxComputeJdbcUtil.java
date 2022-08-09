package com.itdl.util;

import com.alibaba.fastjson.JSONObject;
import com.aliyun.odps.*;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.task.SQLTask;
import com.itdl.common.base.PageResult;
import com.itdl.common.base.ResultCode;
import com.itdl.common.base.TableColumnMetaInfo;
import com.itdl.common.base.TableMetaInfo;
import com.itdl.common.exception.BizException;
import com.itdl.conn.param.MaxComputeJdbcConnParam;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

import java.sql.*;
import java.util.*;

/**
 * @Description
 * @Author itdl
 * @Date 2022/08/08 14:26
 */
@Slf4j
public class MaxComputeJdbcUtil {
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

    /**
     * 根据表名称获取字段列表
     * @return 表信息列表
     */
    public List<TableColumnMetaInfo> getFieldByTableName(String tableName){
        List<TableColumnMetaInfo> resultList = new ArrayList<>();
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            // 创建statement 使用SQL直接查询
            statement = conn.createStatement();
            // 执行查询语句
            final String execSql = String.format(SELECT_FIELD_BY_TABLE_SQL, tableName);
            resultSet = statement.executeQuery(execSql);
            while (resultSet.next()){
                final String columnName = resultSet.getString("column_name");
                final String columnComment = resultSet.getString("column_comment");
                final TableColumnMetaInfo info = new TableColumnMetaInfo(tableName, columnName, columnComment);
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





    /**
     * 获取数据库的连接
     * @return 初始化好的连接
     */
    public Connection getConn() {
        return conn;
    }

    /**
     * 关闭连接
     */
    public void close(){
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        final MaxComputeJdbcConnParam connParam = new MaxComputeJdbcConnParam();
        connParam.setAliyunAccessId("您的阿里云账号aceessId");
        connParam.setAliyunAccessKey("您的阿里云账号aceessKey");
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
}
