package com.itdl.common.base;

import com.itdl.common.enums.BaseEnums;
import lombok.Getter;

/**
 * @Description
 * @Author itdl
 * @Date 2022/08/08 10:40
 */
@Getter
public enum ResultCode implements BaseEnums<String, String> {
    /**通用业务返回码定义，系统-编码*/
    SUCCESS("TEST-000000", "success"),
    MAX_COMPUTE_ENDPOINT_ERR("TEST-000001", "MaxCompute API地址错误"),
    MAX_COMPUTE_PROJECT_ERR("TEST-000002", "MaxCompute 项目不存在或错误"),
    MAX_COMPUTE_UNAME_ERR("TEST-000003", "MaxCompute 用户名错误"),
    MAX_COMPUTE_PWD_ERR("TEST-000004", "MaxCompute 密码错误"),
    MAX_COMPUTE_SQL_EXEC_ERR("TEST-000005", "MaxCompute SQL执行出错"),
    MAX_COMPUTE_JDBC_DRIVE_LOAD_ERR("TEST-000006", "MaxCompute JDBC驱动加载失败"),
    SYSTEM_INNER_ERR("TEST-100000", "系统内部错误"),
    ;

    /**键和值定义为code, value 实现BaseEnums+@Getter完成get方法*/
    private final String code;
    private final String value;

    ResultCode(String code, String value) {
        this.code = code;
        this.value = value;
    }
}
