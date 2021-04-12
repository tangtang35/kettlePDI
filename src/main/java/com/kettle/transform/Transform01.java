package com.kettle.transform;

import com.kettle.model.DatabaseInfo;
import com.kettle.processplugin.DataProcessMeta;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.tableinput.TableInputMeta;
import org.pentaho.di.trans.steps.tableoutput.TableOutputMeta;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Transform01 {

    private static PluginRegistry pluginRegistry = PluginRegistry.getInstance();
    //存储获取的字段
    private static String[] filedNames = {"EMAIL"};

    /**
     * 构建数据库配置
     * @param databaseInfo
     * @return 数据库配置
     */
    public static DatabaseInfo buildDatabaseInfo(DatabaseInfo databaseInfo, Boolean srcOrTar) {
        if(srcOrTar){
            databaseInfo.setName("test");
            databaseInfo.setDatabaseType("oracle");
            databaseInfo.setAccessType(0);
            databaseInfo.setHostName("172.19.1.101");
            databaseInfo.setPort("1521");
            databaseInfo.setDbName("orcl");
            databaseInfo.setDisplayName("172.19.1.202:1521/orcl");
            databaseInfo.setPassword("Ceshi123");
            databaseInfo.setSupportBooleanDataType(true);
            databaseInfo.setSupportTimestampDataType(true);
            databaseInfo.setUsername("scott");
            databaseInfo.setSchemaName("scott");
            databaseInfo.setTableName("BW_2_002");
            databaseInfo.setSql("select * from \"SCOTT\".\"BW_2_002\"");
            Map<String,String> map = new HashMap<String, String>();
            map.put("SUPPORTS_BOOLEAN_DATA_TYPE","Y");
            map.put("SUPPORTS_TIMESTAMP_DATA_TYPE","Y");
            databaseInfo.setProperty(map);
            //构建DatabaeMeta
            DatabaseMeta databaseMeta = createDatabaseMeta(databaseInfo);
            databaseInfo.setDatabaseMeta(databaseMeta);
            return databaseInfo;
        }else {
            databaseInfo.setName("test");
            databaseInfo.setDatabaseType("oracle");
            databaseInfo.setAccessType(0);
            databaseInfo.setHostName("172.19.1.102");
            databaseInfo.setPort("1521");
            databaseInfo.setDbName("orcl");
            databaseInfo.setDisplayName("172.19.1.202:1521/orcl");
            databaseInfo.setPassword("Ceshi123");
            databaseInfo.setSupportBooleanDataType(true);
            databaseInfo.setSupportTimestampDataType(true);
            databaseInfo.setUsername("scott");
            databaseInfo.setSchemaName("scott");
            databaseInfo.setTableName("BW_2_002");
            Map<String,String> map = new HashMap<String, String>();
            map.put("SUPPORTS_BOOLEAN_DATA_TYPE","Y");
            map.put("SUPPORTS_TIMESTAMP_DATA_TYPE","Y");
            databaseInfo.setProperty(map);
            //构建DatabaeMeta
            DatabaseMeta databaseMeta = createDatabaseMeta(databaseInfo);
            databaseInfo.setDatabaseMeta(databaseMeta);
            return databaseInfo;
        }
    }

    /**
     * 获取转换步骤
     *
     * @param srcDatabaseInfo 源数据库配置
     * @param tarDatabaseInfo 目标数据库配置
     * @return 转换步骤配置对象
     */
    public TransMeta getTransMeta(DatabaseInfo srcDatabaseInfo,DatabaseInfo tarDatabaseInfo) {
        StepMeta inputStep;
        StepMeta outputStep;
        StepMeta dataProcessStep;
        //给每个步骤生成一个标识
        PluginRegistry pluginRegistry = PluginRegistry.getInstance();
        //初始化一个转换
        TransMeta transMeta = gainTransMeta(srcDatabaseInfo.getTableName());
        //构建输入步骤
        inputStep = createInputTransMeta(transMeta,srcDatabaseInfo);
        //数据处理步骤
        //dataProcessStep = createDataProcessTransMeta(transMeta, srcDatabaseInfo);
        //构建输出步骤
        outputStep = createOutputTransMeta(transMeta, tarDatabaseInfo);
        //transMeta.addTransHop(new TransHopMeta(inputStep,dataProcessStep));
        transMeta.addTransHop(new TransHopMeta(inputStep,outputStep));
        transMeta.addTransHop(new TransHopMeta(inputStep,outputStep));
        //transMeta.setSizeRowset(50000);
        return transMeta;
    }

    /**
     * 创建DatabaseMeta对象
     * @param databaseInfo
     * @return DatabaseMeta
     */
    public static DatabaseMeta createDatabaseMeta(DatabaseInfo databaseInfo){
        DatabaseMeta databaseMeta = new DatabaseMeta();
        databaseMeta.setName(databaseInfo.getName());
        databaseMeta.setDatabaseType(databaseInfo.getDatabaseType());
        databaseMeta.setAccessType(databaseInfo.getAccessType());
        databaseMeta.setHostname(databaseInfo.getHostName());
        databaseMeta.setDBPort(databaseInfo.getPort());
        databaseMeta.setDBName(databaseInfo.getDbName());
        databaseMeta.setDisplayName(databaseInfo.getDisplayName());
        databaseMeta.setPassword(databaseInfo.getPassword());
        databaseMeta.setSupportsBooleanDataType(databaseInfo.getSupportBooleanDataType());
        databaseMeta.setSupportsTimestampDataType(databaseInfo.getSupportTimestampDataType());
        databaseMeta.setUsername(databaseInfo.getUsername());
        //set Property
        Set<Map.Entry<String, String>> entries = databaseInfo.getProperty().entrySet();
        for (Map.Entry<String, String> entry : entries) {
            String key = entry.getKey();
            String value = entry.getValue();
            databaseMeta.getAttributes().setProperty(key,value);
        }
        return databaseMeta;
    }

    /**
     * 创建输出步骤配置
     *
     * @param databaseInfo
     * @return 输出步骤配置
     */
    private StepMeta createOutputTransMeta(TransMeta transMeta,DatabaseInfo databaseInfo) {
        //创建一个输出元对象
        TableOutputMeta tableOutputMeta = new TableOutputMeta();
        tableOutputMeta.setDatabaseMeta(databaseInfo.getDatabaseMeta());
        tableOutputMeta.setSchemaName(databaseInfo.getSchemaName());
        tableOutputMeta.setTableName(databaseInfo.getTableName());
        //使用批量插入
        tableOutputMeta.setUseBatchUpdate(true);
        tableOutputMeta.setCommitSize(5000);
        String tableOutputId = pluginRegistry.getPluginId(StepPluginType.class, tableOutputMeta);
        tableOutputMeta.getDatabaseMeta().getAttributes().setProperty("SUPPORTS_BOOLEAN_DATA_TYPE","Y");
        tableOutputMeta.getDatabaseMeta().getAttributes().setProperty("SUPPORTS_TIMESTAMP_DATA_TYPE","Y");
        StepMeta outputStepMeta = new StepMeta(tableOutputId, transMeta.getName() + "表输出步骤", tableOutputMeta);
        outputStepMeta.setCopiesString("2");
        transMeta.addStep(outputStepMeta);
        return outputStepMeta;
    }

    /**
     * 创建输入步骤配置
     *
     * @param databaseInfo
     * @return 输入步骤配置
     */
    public StepMeta createInputTransMeta(TransMeta transMeta,DatabaseInfo databaseInfo) {
        transMeta.addDatabase(databaseInfo.getDatabaseMeta());
        //创建一个输入元对象
        TableInputMeta tableInputMeta = new TableInputMeta();
        //给表添加一个Database连接数据库
        tableInputMeta.setDatabaseMeta(databaseInfo.getDatabaseMeta());
        tableInputMeta.setSQL(databaseInfo.getSql());
        tableInputMeta.getDatabaseMeta().getAttributes().setProperty("SUPPORTS_BOOLEAN_DATA_TYPE","Y");
        tableInputMeta.getDatabaseMeta().getAttributes().setProperty("SUPPORTS_TIMESTAMP_DATA_TYPE","Y");
        String tableInputId = pluginRegistry.getPluginId(StepPluginType.class, tableInputMeta);
        //将转换添加到步骤当中
        StepMeta inputStepMeta = new StepMeta(tableInputId, transMeta.getName() + "表输入步骤", tableInputMeta);
        //将步骤添加到转换
        transMeta.addStep(inputStepMeta);
        return inputStepMeta;
    }


    /**
     * 创建数据处理步骤配置
     *
     * @param databaseInfo
     * @return 输入步骤配置
     */
    public StepMeta createDataProcessTransMeta(TransMeta transMeta,DatabaseInfo databaseInfo) {
        DataProcessMeta dataProcess = new DataProcessMeta();
        String dataProcessPluginId = pluginRegistry.getPluginId(StepPluginType.class, dataProcess);
        dataProcess.allocate(filedNames.length);
        dataProcess.setFieldInStream(filedNames);
        StepMeta dataProcessMetaStep = new StepMeta(dataProcessPluginId,"data process",dataProcess);
        //将步骤添加到转换
        transMeta.addStep(dataProcessMetaStep);
        return dataProcessMetaStep;
    }

    /**
     * 初始化一个转换
     *
     * @return
     */
    public TransMeta gainTransMeta(String tableName) {
        //初始化一个转换
        TransMeta transMeta = new TransMeta();
        transMeta.setName(tableName);
        return transMeta;
    }
}
