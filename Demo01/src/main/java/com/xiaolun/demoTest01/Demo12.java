package com.xiaolun.demoTest01;

import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.junit.Test;

public class Demo12 {

	@Test
	public void firstKeyOnlyFilter() throws Exception {
		//打印第一行key的过滤器（只讲age那个行键打印出来）
		FirstKeyOnlyFilter filter1 = new FirstKeyOnlyFilter();
		HBaseUtil.showFilter(filter1, "t1");
	}
}
