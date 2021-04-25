package com.kettle.processplugin;

import com.kettle.model.DesenRule;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;
import org.pentaho.metastore.api.IMetaStore;

import java.util.List;
import java.util.Map;

/**
 * DataProcessMeta 类主要用来存储用户的配置数据
 * @author Administrator
 *
 */
public class DataProcessMeta extends BaseStepMeta implements StepMetaInterface {
	//?
	private static Class<?> PKG = DataProcessMeta.class;
	//脱敏字段数组
	private String[] fieldInStream;
	private String[] fieldOutStream;
	private DesenRule desenRule;

	//添加脱敏字段,<字段名称，对应的算法>
	private Map<String,String> fieldDataType;

	//数据库类型
	private String dbType;

	public String getDbType() {
		return dbType;
	}

	public void setDbType(String dbType) {
		this.dbType = dbType;
	}

	public Map<String, String> getFieldDataType() {
		return fieldDataType;
	}

	public void setFieldDataType(Map<String, String> fieldDataType) {
		this.fieldDataType = fieldDataType;
	}

	public DesenRule getDesenRule() {
		return desenRule;
	}

	public void setDesenRule(DesenRule desenRule) {
		this.desenRule = desenRule;
	}

	public String[] getFieldInStream() {
		return fieldInStream;
	}

	public void setFieldInStream(String[] fieldInStream) {
		this.fieldInStream = fieldInStream;
	}

	public String[] getFieldOutStream() {
		return fieldOutStream;
	}

	public void setFieldOutStream(String[] fieldOutStream) {
		this.fieldOutStream = fieldOutStream;
	}

	//初始化定义的变量
	public void allocate(int nrkeys){
	    this.fieldInStream = new String[nrkeys];
	    this.fieldOutStream = new String[nrkeys];
	}

	public void setDefault() {
		int nrkeys = 0;
	    allocate(nrkeys);
	}
	/**
	 * getStep、getStepData和getDialogClassName()方法提供了与这个步骤里其它三个接口之间的桥梁
	 这个接口里还定义了几个方法来说明这四个接口如何结合到一起。
	 String getDialogClassName():用来描述实现了StepDialogInterace接口的对话框类的名字。如果这个方法返回
	 了null，调用类会根据实现了StepMetaInterface接口的类的类名和包名来自动生成对话框类的名字。
	 StepInterface getStep():创建一个实现了StepInterface接口的类。
	 StepInterface getStepData():创建一个实现了StepDataInterface接口的类
	 */
	public StepInterface getStep(StepMeta stepMeta,
                                 StepDataInterface stepDataInterface, int copyNr,
                                 TransMeta transMeta, Trans trans) {
		return new DataProcess(stepMeta, stepDataInterface, copyNr, transMeta, trans);
	}

	public StepDataInterface getStepData() {
		return new DataProcessData();
	}

	@Override
	public void readRep(Repository rep, IMetaStore metaStore, ObjectId idStep,
                        List<DatabaseMeta> databases) throws KettleException {
		try
	    {
	      int nrkeys = rep.countNrStepAttributes(idStep, "in_stream_name");
	      allocate(nrkeys);
	      for (int i = 0; i < nrkeys; i++)
	      {
	        this.fieldInStream[i] = Const.NVL(rep.getStepAttributeString(idStep, i, "in_stream_name"), "");
	        this.fieldOutStream[i] = Const.NVL(rep.getStepAttributeString(idStep, i, "out_stream_name"), "");
	      }
	    }
	    catch (Exception e)
	    {
	      throw new KettleException(BaseMessages.getString(PKG, "RedisInputMeta.Exception.UnexpectedErrorReadingStepInfo", new String[0]), e);
	    }
	}

	@Override
	public void saveRep(Repository rep, IMetaStore metaStore,
                        ObjectId idTransformation, ObjectId idStep)
			throws KettleException {
		try
	    {
	      for (int i = 0; i < this.fieldInStream.length; i++)
	      {
	        rep.saveStepAttribute(idTransformation, idStep, i, "in_stream_name", this.fieldInStream[i]);
	        rep.saveStepAttribute(idTransformation, idStep, i, "out_stream_name", this.fieldOutStream[i]);
	      }
	    }
	    catch (Exception e)
	    {
	      throw new KettleException(BaseMessages.getString(PKG, "RedisInputMeta.Exception.UnexpectedErrorSavingStepInfo", new String[0]), e);
	    }
	}

	@Override
	public void getFields(RowMetaInterface inputRowMeta, String name,
                          RowMetaInterface[] info, StepMeta nextStep, VariableSpace space,
                          Repository repository, IMetaStore metaStore)
			throws KettleStepException {
		int nrFields = this.fieldInStream == null ? 0: this.fieldInStream.length;
		for (int i = 0; i < nrFields; i++) {
			String fieldName = space.environmentSubstitute(this.fieldOutStream[i]);

			if (!Utils.isEmpty(this.fieldOutStream[i])) {
				//值的元数据使用ValueMetaInterface接口描述数据流里的一个字段。这个接口里定义了字段的名字、数据类型、长度、精度，等等
				ValueMetaInterface valueMeta = new ValueMetaString(fieldName);
				valueMeta.setOrigin(name);

				ValueMetaInterface sourceField = inputRowMeta.searchValueMeta(this.fieldInStream[i]);
				if (sourceField != null) {
					valueMeta.setStringEncoding(sourceField.getStringEncoding());
				}
				//修改inputRowMeta对象，与输出格式匹配
				inputRowMeta.addValueMeta(valueMeta);
			} else {
				ValueMetaInterface valueMeta = inputRowMeta.searchValueMeta(this.fieldInStream[i]);
				if (valueMeta != null) {
					valueMeta.setStorageType(0);
				}
			}
		}
	}

}
