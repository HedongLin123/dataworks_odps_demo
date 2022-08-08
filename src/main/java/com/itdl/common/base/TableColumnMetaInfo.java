package com.itdl.common.base;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Description 数据库的表字段的元数据信息
 * @Author itdl
 * @Date 2022/08/08 11:15
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableColumnMetaInfo {
    /**表名称*/
    private String tableName;
    /**表字段名称*/
    private String fieldName;
    /**表注释*/
    private String comment;
}
