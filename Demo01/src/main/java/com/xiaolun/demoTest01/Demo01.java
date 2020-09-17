package com.xiaolun.demoTest01;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class Demo01 {
	/**
	 * select *from t1 where age <= 18
	 */
	@Test
	public void singleColumnvalueFilter() throws Exception {
		/**
		 * 1、创建单列值过滤器
		 * Bytes.toBytes("18") 要带上引号""，不然查不到数据
		 */
		SingleColumnValueFilter filter1 =
				new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("age"),
						CompareFilter.CompareOp.LESS_OR_EQUAL, Bytes.toBytes("18"));
		/**
		 * 设置单值过滤，如果没有这个属性列，就不计算它
		 * 假如某个列键中的列族中只有name属性，没有age属性
		 * 设置下面的属性后，该列键就不参加计算，此时不会输出，
		 * 否则会输出
		 */
		filter1.setFilterIfMissing(true);
		//2、扫描整个表
		Scan scan = new Scan();
		//3、设置过滤器
		scan.setFilter(filter1);
		//4、获取表
		Table table = HBaseUtil.getTable("t1");
		//5、打印
		HBaseUtil.showScan(table, scan);
		//6、释放资源
		HBaseUtil.close(table);
	}
}
