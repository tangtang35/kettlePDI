package com.kettle.processplugin;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;

import java.util.Map;

/**
 * 自定义插件
 * @author Administrator
 *
 */

public class DataProcess extends BaseStep implements StepInterface {

	private static Class<?> PKG = DataProcess.class;
	private DataProcessMeta meta;
	private DataProcessData data;
	public DataProcess(StepMeta stepMeta, StepDataInterface stepDataInterface,
                       int copyNr, TransMeta transMeta, Trans trans) {
		super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
	}
	/**
	 * @Author osguang
	 * @Description //数据处理方法
	 * @Date 2018/8/15
	 * @Param
	 * @return
	 **/
	@Override
	public boolean processRow(StepMetaInterface smi, StepDataInterface sdi)
			throws KettleException {
		this.meta = ((DataProcessMeta)smi);
		this.data = ((DataProcessData)sdi);
		Object[] r = getRow(); //获得流传过来的一行数据，即上个组件传过来的
	    if (r == null){
	      setOutputDone(); //如果为空或者没有数据了就完成
	      return false;
	    }
	    
	    if(this.first){//如果是第一行数据，则可以进行初始化的工作
	    	this.first = false;
	    	this.data.outputRowMeta = getInputRowMeta().clone();
	        this.data.inputFieldsNr = this.data.outputRowMeta.size();
	        //给输出数据增加了一个字段
	        this.meta.getFields(this.data.outputRowMeta, getStepname(), null, null, this, this.repository, this.metaStore);
	        this.data.numFields = this.meta.getFieldInStream().length;
	        this.data.inStreamNrs = new int[this.data.numFields];
	        this.data.outStreamNrs = new String[this.data.numFields];
	        String[] fields = this.meta.getFieldInStream();
	        System.out.println("需要脱敏的字段名称："+fields.toString());
	        for (int i = 0; i < this.data.numFields; i++) {
	        	      this.data.inStreamNrs[i] = getInputRowMeta().indexOfValue(this.meta.getFieldInStream()[i]);
	        	       if (this.data.inStreamNrs[i] < 0) {
	        	          throw new KettleStepException(BaseMessages.getString(PKG, "ReplaceString.Exception.FieldRequired", new String[] {this.meta
	        	           .getFieldInStream()[i] }));
	        	       }
	        }
	        
	        for (int i = 0; i < this.data.numFields; i++) {
	            this.data.outStreamNrs[i] = this.meta.getFieldOutStream()[i];
	            System.out.println(this.meta.getFieldOutStream()[i]);
	        }
	        
	    }
	    int j =0;
	    for (int i = 0; i < this.data.numFields; i++) {
	    	//数据库类型
			String dbType = this.meta.getDbType();
			//当前数据的字段名
			String fieldName = this.data.outputRowMeta.getValueMeta(this.data.inStreamNrs[i]).getName();
			//字段类型
			int fieldType = this.data.outputRowMeta.getValueMeta(this.data.inStreamNrs[i]).getType();
			//获得脱敏前的当前数据
	    	Object beforValue = getValue(r, data.inStreamNrs[i],getInputRowMeta());
	    	//脱敏的字段和算法
			Map<String,String> fieldDataType = this.meta.getFieldDataType();
			//算法
			String str = fieldDataType.get(fieldName);
			System.out.println("数据库类型："+dbType+"----字段名："+fieldName+"----字段类型："+fieldType+"------数据："+beforValue+"-----算法："+str);
			Object afterValue = beforValue;
			if (Utils.isEmpty(this.data.outStreamNrs[i])) {
				r[this.data.inStreamNrs[i]] = "tcmeng"; //设置脱敏后的值
				this.data.outputRowMeta.getValueMeta(this.data.inStreamNrs[i])
						.setStorageType(0);
			} else {
				r[(this.data.inputFieldsNr + j++)] = afterValue;
			}

	    }
	    putRow(this.data.outputRowMeta, r); //将处理后的一行数据传给下一个组件处理
	    return true;
	}
	@Override
	public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
		this.meta = ((DataProcessMeta)smi);
		this.data = ((DataProcessData)sdi);
		super.dispose(smi, sdi);
	}

	//获取一行的某值
	private Object getValue(Object[] r, int index, RowMetaInterface rowMetaInterface) throws KettleValueException {
		
		ValueMetaInterface valueMeta = rowMetaInterface.getValueMeta(index);
	    Object value = r[index];
	    switch (valueMeta.getType())
	    {
	    case 2: 
	      value = rowMetaInterface.getString(r, index);
	      break;
	    case 1: 
	      value = rowMetaInterface.getNumber(r, index);
	      break;
	    case 5: 
	      value = rowMetaInterface.getInteger(r, index);
	      break;
	    case 3: 
	      value = rowMetaInterface.getDate(r, index);
	      break;
	    case 9: 
	      value = rowMetaInterface.getDate(r, index);
	      break;
	    case 6: 
	      value = rowMetaInterface.getBigNumber(r, index);
	      break;
	    case 4: 
	      value = rowMetaInterface.getBoolean(r, index);
	      break;
	    case 8: 
	      value = rowMetaInterface.getBinary(r, index);
	      break;
	    case 7: 
	    default: 
	      value = rowMetaInterface.getString(r, index);
	    }
	    return value;
	}
}
