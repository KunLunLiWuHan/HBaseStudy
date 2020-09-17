package com.xiaolun.demoTest01;

import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class Demo05 {

	//select *from t1 where age = 18
	@Test
	public void binaryComparator() throws Exception {
		//二进制比较器,加引号的"18"，才是一个正常的字节。能够正常输出
		BinaryComparator binaryComparator = new BinaryComparator(Bytes.toBytes("18"));
		SingleColumnValueFilter filter1 =
				new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("age"),
						CompareFilter.CompareOp.EQUAL, binaryComparator);

		filter1.setFilterIfMissing(true);
		HBaseUtil.showFilter(filter1, "t1");
	}
}
