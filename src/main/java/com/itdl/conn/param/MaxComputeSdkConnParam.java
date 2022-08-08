package com.itdl.conn.param;

import lombok.Data;

/**
 * @author itdl
 * @description maxCompute使用SDK的连接参数
 * @date 2022/08/08 10:07
 */
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