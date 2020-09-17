package com.xiaolun;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

public class HBaseProcessor extends BaseRegionObserver {
	/**
	 * 1、该方法会在put命令执行之前被调用
	 * 2、Put put：就是你的put命令，这个命令里面封装了你执行时候的数据
	 * 3、put 'guanzhu','canglaoshi','cf:name','xiaoxi'
	 */
	@Override
	public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
		//1、获取行键
		byte[] rowKey = put.getRow();
		//2、获取name属性对应的多个版本的值
		List<Cell> cells = put.get(Bytes.toBytes("cf"), Bytes.toBytes("name"));
		//3、获取最新版本
		Cell cell = cells.get(0);
		//4、获取到值
		byte[] value = CellUtil.cloneValue(cell);
		//5、创建新的Put对象,成为一个新行键
		Put new_Put = new Put(value);
		new_Put.addColumn(CellUtil.cloneFamily(cell), Bytes.toBytes("start"), rowKey);
		//6、提交
		Table table = HBaseUtil.getTable("fans");
		table.put(new_Put);
		//7、释放资源
		HBaseUtil.close(table);
	}
}
