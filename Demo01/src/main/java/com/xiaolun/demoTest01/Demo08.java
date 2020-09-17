package com.xiaolun.demoTest01;

import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class Demo08 {

	@Test
	public void familyFilter() throws Exception {
		//列名过滤器,将列名以a开头的查出来
		BinaryPrefixComparator prefixComparator = new BinaryPrefixComparator(Bytes.toBytes("a"));
		QualifierFilter filter1 = new QualifierFilter(CompareFilter.CompareOp.EQUAL, prefixComparator);
		HBaseUtil.showFilter(filter1, "t1");
	}
}
