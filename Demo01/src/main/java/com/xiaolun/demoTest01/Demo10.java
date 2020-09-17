package com.xiaolun.demoTest01;

import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class Demo10 {

	//将age --> name 列数据全部查出来
	@Test
	public void columnRangeFilter() throws Exception {
		//列范围过滤器 false:表示小于；true：表示大于等于
		ColumnRangeFilter filter = new ColumnRangeFilter(Bytes.toBytes("age"), true, Bytes.toBytes("name"), false);
		HBaseUtil.showFilter(filter, "t1");
	}
}
