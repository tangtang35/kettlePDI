package com.kettle.model;

import lombok.Data;
import org.pentaho.di.core.database.DatabaseMeta;

import java.util.Map;

@Data
public class DatabaseInfo {

    //名称，随意写
    private String name;

    //数据库类型
    private String databaseType;

    //数据库连接方式，0：jdbc，1：odbc，2：OCI、3：plugin specific method、4：JNDI
    private Integer accessType;

    //ip地址
    private String hostName;

    //端口
    private String port;

    //库名
    private String dbName;

    //IP地址:端口号/库名
    private String displayName;

    //密码
    private String password;

    //支持Boolean类型
    private Boolean supportBooleanDataType;

    //支持Timestamp类型
    private Boolean supportTimestampDataType;

    //设置参数，例如：是否压缩
    private Map<String,String> property;

    //用户名
    private String username;

    //schema
    private String schemaName;

    //表名
    private String tableName;

    private DatabaseMeta databaseMeta;

    //执行的sql
    private String sql;

}
