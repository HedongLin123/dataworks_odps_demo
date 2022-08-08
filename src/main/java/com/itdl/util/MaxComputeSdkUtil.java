package com.itdl.util;

import com.alibaba.fastjson.JSONObject;
import com.aliyun.odps.*;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.task.SQLTask;
import com.itdl.common.base.PageResult;
import com.itdl.common.base.ResultCode;
import com.itdl.common.base.TableColumnMetaInfo;
import com.itdl.common.base.TableMetaInfo;
import com.itdl.common.exception.BizException;
import com.itdl.conn.param.MaxComputeSdkConnParam;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import java.util.*;

/**
 * @author itdl
 * @description maxCompute操作工具类
 * @date 2022/08/08 10:07
 */
@Slf4j
public class MaxComputeSdkUtil {
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


    /**
     * 根据表名称获取字段列表
     * @return 表信息列表
     */
    public List<TableColumnMetaInfo> getFieldByTableName(String tableName){
        List<TableColumnMetaInfo> resultList = new ArrayList<>();
        try {
            final Table table = odps.tables().get(tableName);
            final TableSchema schema = table.getSchema();
            final List<Column> columns = schema.getColumns();
            final List<Column> partitionColumns = schema.getPartitionColumns();
            for (Column column : columns) {
                final TableColumnMetaInfo info = new TableColumnMetaInfo(tableName, column.getName(), column.getComment());
                resultList.add(info);
            }
            for (Column column : partitionColumns) {
                final TableColumnMetaInfo info = new TableColumnMetaInfo(tableName, column.getName(), column.getComment());
                resultList.add(info);
            }
            return resultList;
        } catch (Exception e) {
            e.printStackTrace();
            throw new BizException(ResultCode.MAX_COMPUTE_SQL_EXEC_ERR);
        }
    }





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

    public static void main(String[] args) {
        // 构建连接参数
        final MaxComputeSdkConnParam connParam = new MaxComputeSdkConnParam();
        connParam.setAliyunAccessId("您的阿里云账号aceessId");
        connParam.setAliyunAccessKey("您的阿里云账号aceessKey");
        connParam.setProjectName("项目名");

        // 实例化工具类
        final MaxComputeSdkUtil sdkUtil = new MaxComputeSdkUtil(connParam);

        // 查询所有表
        final List<TableMetaInfo> tableInfos = sdkUtil.getTableInfos();
        for (TableMetaInfo tableInfo : tableInfos) {
            System.out.println(tableInfo.getTableName());
        }

        // 获取字段信息
        final String tableName = tableInfos.get(new Random().nextInt(tableInfos.size())).getTableName();
        final List<TableColumnMetaInfo> fields = sdkUtil.getFieldByTableName(tableName);
        for (TableColumnMetaInfo field : fields) {
            System.out.println(field.getFieldName() + "-" + field.getComment());
        }

//        // 分页查询数据
//        final PageResult<Map<String, Object>> page = sdkUtil.pageQueryMap("select * from ods_cust;", 2, 10);
//        System.out.println(page.getTotal());
//        for (Map<String, Object> map : page.getRows()) {
//            System.out.println(JSONObject.toJSONString(map));
//        }
    }
}
