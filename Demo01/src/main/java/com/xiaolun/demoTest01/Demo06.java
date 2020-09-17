package com.xiaolun.demoTest01;

import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class Demo06 {

	//select *from t1 where name like "z%"
	@Test
	public void binaryPrefixComparator() throws Exception {
		//二进制前置比较器
		BinaryPrefixComparator prefixComparator = new BinaryPrefixComparator(Bytes.toBytes("z"));
		SingleColumnValueFilter filter1 =
				new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("name"),
						CompareFilter.CompareOp.EQUAL, prefixComparator);

		filter1.setFilterIfMissing(true);
		HBaseUtil.showFilter(filter1, "t1");
	}
}
