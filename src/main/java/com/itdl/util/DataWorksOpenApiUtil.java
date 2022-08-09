package com.itdl.util;

import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.dataworks_public.model.v20200518.*;
import com.aliyuncs.exceptions.ClientException;
import com.aliyuncs.profile.DefaultProfile;
import com.itdl.common.base.ResultCode;
import com.itdl.common.base.TableMetaInfo;
import com.itdl.common.exception.BizException;
import com.itdl.conn.param.DataWorksOpenApiConnParam;
import com.itdl.conn.param.MaxComputeJdbcConnParam;
import com.itdl.conn.param.MaxComputeSdkConnParam;
import org.springframework.util.ObjectUtils;

import java.util.List;

/**
 * @Description dataworks open api util
 * @Author itdl
 * @Date 2022/08/09 15:18
 */
public class DataWorksOpenApiUtil {
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


    /**
     * 获取数据库信息
     */
    public GetMetaDBInfoResponse.Data getDbInfo() throws ClientException {
        final GetMetaDBInfoRequest request = new GetMetaDBInfoRequest();

        // 设置 app guid 格式为 odps.{projectName}
        request.setAppGuid(String.join(".", connParam.getDatasourceType(), connParam.getProject()));
        // 设置数据库类型
        request.setDataSourceType(connParam.getDatasourceType());

        // 使用客户端发起请求
        GetMetaDBInfoResponse res = client.getAcsResponse(request);

        // 获取结果数据
        return res.getData();
    }


    /**
     * 获取数据库下的所有表信息
     */
    public List<GetMetaDBTableListResponse.Data.TableEntityListItem> getDbAllTableInfo() throws ClientException {
        // 阿里云限制：每页显示的条数，默认为10条，最大为100条。
        return getDbAllTableInfo(100);
    }

    /**
     * 获取数据库下的所有表信息 指定每页展示条数 条数最大为100
     */
    public List<GetMetaDBTableListResponse.Data.TableEntityListItem> getDbAllTableInfo(Integer pageSize) throws ClientException {
        pageSize = setPageSize(pageSize);
        GetMetaDBTableListRequest request = new GetMetaDBTableListRequest();

        // 设置 app guid 格式为 odps.{projectName}
        request.setAppGuid(String.join(".", connParam.getDatasourceType(), connParam.getProject()));
        // 设置数据库类型
        request.setDataSourceType(connParam.getDatasourceType());

        //第1页
        request.setPageNumber(1);

        //每页大小
        request.setPageSize(pageSize);

        // 使用客户端发起请求
        GetMetaDBTableListResponse res = client.getAcsResponse(request);

        // 获取数据
        final GetMetaDBTableListResponse.Data data = res.getData();

        // 获取总记录数
        final Long totalCount = data.getTotalCount();

        List<GetMetaDBTableListResponse.Data.TableEntityListItem> resultList = data.getTableEntityList();

        // 计算能分几页
        long pages = totalCount % pageSize == 0 ? (totalCount / pageSize) : (totalCount / pageSize) + 1;
        // 只有1页 直接返回
        if (pages <= 1){
            return resultList;
        }

        // 分页数据 从第二页开始查询
        for (int i = 2; i <= pages; i++) {
            //第1页
            request.setPageNumber(i);
            //每页大小
            request.setPageSize(pageSize);
            // 发起请求
            res = client.getAcsResponse(request);
            final List<GetMetaDBTableListResponse.Data.TableEntityListItem> tableEntityList = res.getData().getTableEntityList();
            if (!ObjectUtils.isEmpty(tableEntityList)){
                resultList.addAll(tableEntityList);
            }
        }

        return resultList;
    }


    /**
     * 校验表是否存在
     * @param tableName 表名
     * @return true 存在 false 不存在
     */
    public Boolean checkTableExists(String tableName) throws ClientException {
        CheckMetaTableRequest request = new CheckMetaTableRequest();
        //odps table  guid，格式odps.{projectName}.{tableName}
        request.setTableGuid(String.join(".", connParam.getDatasourceType(), connParam.getProject(), tableName));
        //资源类型
        request.setDataSourceType(connParam.getDatasourceType());
        // 发起请求
        CheckMetaTableResponse res = client.getAcsResponse(request);
        //表是否存在
        return res.getData();
    }


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


    /**
     * 设置分页大小，防止分页参数传错误
     * @param pageSize 分页每页记录数  最大为100 最小为1
     * @return 限制后的每页记录数
     */
    private Integer setPageSize(Integer pageSize){
        if (pageSize == null || pageSize < 0){
            pageSize = 1;
        }else if (pageSize > 100) {
            pageSize = 100;
        }
        return pageSize;
    }

    /**
     * 构建一个Client 用于向open api发起请求
     * @return client
     */
    private IAcsClient buildClient() {
        // 1、获取客户端profile
        final String region = connParam.getRegion();
        final String accessKeyId = connParam.getAliyunAccessId();
        final String secret = connParam.getAliyunAccessKey();
        final String endPoint = connParam.getEndPoint();
        final DefaultProfile profile = DefaultProfile.getProfile(region, accessKeyId, secret);

        // 2、添加端点
        try {
            DefaultProfile.addEndpoint(region, "dataworks-public", endPoint);
        } catch (Exception e) {
            e.printStackTrace();
            throw new BizException(ResultCode.DATA_WORKS_ENDPOINT_ERR);
        }

        // 3、创建客户端
        return new DefaultAcsClient(profile);
    }


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


    public static void main(String[] args) throws ClientException {
        final DataWorksOpenApiConnParam connParam = new DataWorksOpenApiConnParam();
        connParam.setAliyunAccessId("阿里云accessId");
        connParam.setAliyunAccessKey("阿里云accessKey");
        // 区域
        connParam.setRegion("cn-chengdu");
        // 项目名
        connParam.setProject("项目名");
        // dev 开发环境 prod生产环境
        connParam.setProjectEnv("dev");
        connParam.setDatasourceType("odps");
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





}
