package com.kettle;

import com.kettle.init.InitEnviroment;
import com.kettle.model.DatabaseInfo;
import com.kettle.transform.Transform;
import com.kettle.transform.Transform01;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;

public class KettleEntrance {

    public static void main(String[] args) throws KettleException {

        //初始化Kettle环境
        InitEnviroment.init();

        Runnable runnable01 = () -> {
            //01
            long start1 = System.currentTimeMillis();
            DatabaseInfo srcDatabaseInfo01 = new DatabaseInfo();
            DatabaseInfo tarDatabaseInfo01 = new DatabaseInfo();
            srcDatabaseInfo01 = Transform.buildDatabaseInfo(srcDatabaseInfo01, true);
            tarDatabaseInfo01 = Transform.buildDatabaseInfo(tarDatabaseInfo01, false);

            Transform transformDemo01 = new Transform();
            //获取转换配置
            TransMeta transMeta01 = transformDemo01.getTransMeta(srcDatabaseInfo01, tarDatabaseInfo01);
            Trans trans = new Trans(transMeta01);
            try {
                trans.prepareExecution(null);
            } catch (KettleException e) {
                e.printStackTrace();
            }
            try {
                trans.startThreads();
            } catch (KettleException e) {
                e.printStackTrace();
            }
            trans.waitUntilFinished();
            if (trans.getErrors() != 0) {
                //执行失败
                System.out.println(trans.getErrors());
            } else {
                System.out.println("执行成功：" + (int) trans.getLastProcessed());
            }
            long end1 = System.currentTimeMillis();
            System.out.println(end1 - start1);
        };
        new Thread(runnable01).start();


        Runnable runnable02 = () -> {
            //02
            long start2 = System.currentTimeMillis();
            DatabaseInfo srcDatabaseInfo02 = new DatabaseInfo();
            DatabaseInfo tarDatabaseInfo02 = new DatabaseInfo();
            srcDatabaseInfo02 = Transform01.buildDatabaseInfo(srcDatabaseInfo02, true);
            tarDatabaseInfo02 = Transform01.buildDatabaseInfo(tarDatabaseInfo02, false);

            Transform transformDemo02 = new Transform();
            //获取转换配置
            TransMeta transMeta02 = transformDemo02.getTransMeta(srcDatabaseInfo02, tarDatabaseInfo02);
            Trans trans = new Trans(transMeta02);
            try {
                trans.prepareExecution(null);
            } catch (KettleException e) {
                e.printStackTrace();
            }
            try {
                trans.startThreads();
            } catch (KettleException e) {
                e.printStackTrace();
            }
            trans.waitUntilFinished();
            if (trans.getErrors() != 0) {
                //执行失败
                System.out.println(trans.getErrors());
            } else {
                System.out.println("执行成功：" + (int) trans.getLastProcessed());
            }
            long end2 = System.currentTimeMillis();
            System.out.println(end2 - start2);
        };
        new Thread(runnable02).start();
    }

}
