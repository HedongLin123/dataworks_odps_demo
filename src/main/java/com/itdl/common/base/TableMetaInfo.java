package com.itdl.common.base;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Description 数据库的表的元数据信息
 * @Author itdl
 * @Date 2022/08/08 11:15
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableMetaInfo {
    /**表名称*/
    private String tableName;
    /**表注释*/
    private String comment;
}
