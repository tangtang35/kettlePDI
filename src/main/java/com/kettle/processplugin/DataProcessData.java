package com.kettle.processplugin;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

public class DataProcessData extends BaseStepData implements StepDataInterface {
	public RowMetaInterface inputRowMeta;
	public RowMetaInterface outputRowMeta;
	public int inputFieldsNr; //
	public int[] inStreamNrs;
	public String[] outStreamNrs;
	public int nrFieldsInStream;

	public int numFields;//获取到表字段的列数
	
	public DataProcessData() {
		this.inputFieldsNr = 0;
	}
}
