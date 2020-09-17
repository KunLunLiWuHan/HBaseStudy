package com.xiaolun.demoTest01;

import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class Demo09 {

	@Test
	public void columnPrefixFilter() throws Exception {
		//列名前缀过滤器,将列名以a开头的查出来
		ColumnPrefixFilter filter1 = new ColumnPrefixFilter(Bytes.toBytes("a"));
		HBaseUtil.showFilter(filter1, "t1");
	}

	@Test
	public void multiplecolumnPrefixFilter() throws Exception {
		//列多前缀匹配过滤器,将列名以a和n开头的查出来
		byte [][] prefixes = {Bytes.toBytes("a"),Bytes.toBytes("n")};
		MultipleColumnPrefixFilter prefixFilter = new MultipleColumnPrefixFilter(prefixes);
		HBaseUtil.showFilter(prefixFilter, "t1");
	}
}
