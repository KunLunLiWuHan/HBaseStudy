package com.xiaolun.demoTest01;

import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class Demo04 {

	//select *from t1 where name like %z%
	@Test
	public void SubStringComparator() throws Exception {

		SubstringComparator substringComparator = new SubstringComparator("z");
		SingleColumnValueFilter filter1 =
				new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("name"),
						CompareFilter.CompareOp.EQUAL, substringComparator);

		filter1.setFilterIfMissing(true);
		HBaseUtil.showFilter(filter1,"t1");
	}
}
