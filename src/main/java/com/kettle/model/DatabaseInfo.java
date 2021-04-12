package com.kettle.model;

import lombok.Data;
import org.pentaho.di.core.database.DatabaseMeta;

import java.util.Map;

@Data
public class DatabaseInfo {

    private String name;

    private String databaseType;

    private Integer accessType;

    private String hostName;

    private String port;

    private String dbName;

    private String displayName;

    private String password;

    private Boolean supportBooleanDataType;

    private Boolean supportTimestampDataType;

    private Map<String,String> property;

    private String username;

    private String schemaName;


    private String tableName;

    private DatabaseMeta databaseMeta;

    private String sql;

}
