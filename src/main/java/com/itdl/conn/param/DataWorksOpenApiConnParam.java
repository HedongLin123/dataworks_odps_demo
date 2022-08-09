package com.itdl.conn.param;

import lombok.Data;

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
