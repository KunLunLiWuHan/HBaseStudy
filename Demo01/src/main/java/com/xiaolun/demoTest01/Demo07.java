package com.xiaolun.demoTest01;

import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class Demo07 {

	@Test
	public void familyFilter() throws Exception {
		//列族比较器，将列族中头为i的取出来
		BinaryPrefixComparator prefixComparator = new BinaryPrefixComparator(Bytes.toBytes("i"));
		FamilyFilter filter1 = new FamilyFilter(CompareFilter.CompareOp.EQUAL, prefixComparator);
		HBaseUtil.showFilter(filter1, "t1");
	}
}
