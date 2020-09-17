package com.xiaolun.demoTest01;

import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class Demo11 {

	//查询001行键的数据
	@Test
	public void rowFilter() throws Exception {
		BinaryComparator comparator = new BinaryComparator(Bytes.toBytes("001"));
		//行键比较器
		RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,comparator );
		HBaseUtil.showFilter(filter, "t1");
	}
}
