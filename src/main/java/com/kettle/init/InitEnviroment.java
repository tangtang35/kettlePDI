package com.kettle.init;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;

public class InitEnviroment{

    /**
     * Kettle环境初始化
     */
    public static void init(){
        try {
            KettleEnvironment.init();
        } catch (KettleException e) {
            e.printStackTrace();
        }
    }

}
