package com.xiaolun;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;

import java.io.IOException;
import java.util.Iterator;

public class HBaseUtil {
	private final static String HBASE_KEY = "hbase.zookeeper.quorum";
	private final static String HBASE_VALUE = "hadoop102,hadoop103,hadoop101";

	//将 static中的 connection 提取出来，以便getAdmin能够拿到该对象
	private static Connection connection;

	static {
		try {
			//获取配置对象
			Configuration conf = new Configuration();
			//设置连接HBase的参数
			conf.set(HBASE_KEY, HBASE_VALUE);
			//获取连接对象
			connection = ConnectionFactory.createConnection(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static Admin getAdmin() {
		//异常在这里进行处理，不要抛出去
		try {
			//提取核心对象 admin
			HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
			return admin;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	//获取表封装
	public static Table getTable() {
		//获取发哦默认的表
		return getTable("DefaultTable");
	}

	public static Table getTable(String tableName) {
		Table table = null;
		try {
			//如果为空，返回为null
			if (StringUtils.isEmpty(tableName)) return null;

			//获取table对象
			table = connection.getTable(TableName.valueOf(tableName));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return table;
	}

	//Scan封装
	public static void showScan(Table table, Scan scan) {
		try {
			//结果扫描器
			ResultScanner scanner = table.getScanner(scan);
			//5、迭代
			Iterator<Result> iterator = scanner.iterator();
			while (iterator.hasNext()) {
				Result result = iterator.next();
				HBaseUtil.showResult(result);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	//showResult
	private static void showResult(Result result) {
		//表格扫描器
		CellScanner cellScanner = result.cellScanner();
		try {
			//遍历扫描器
			while (cellScanner.advance()) {
				//获取一个表格
				Cell cell = cellScanner.current();
				//行键 列族 列名 列值
				System.out.println(new String(CellUtil.cloneRow(cell), "utf-8") + "\t" +
						new String(CellUtil.cloneFamily(cell), "utf-8")
						+ "\t" + new String(CellUtil.cloneQualifier(cell), "utf-8") + "\t" +
						new String(CellUtil.cloneValue(cell), "utf-8"));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	/**
	 * 展示过滤器的输出结果
	 *
	 * @param filter    过滤器
	 * @param tableName 表名
	 */
	public static void showFilter(Filter filter, String tableName) {
		//扫描整个表
		Scan scan = new Scan();
		//设置过滤器
		scan.setFilter(filter);
		//获取表 （主要）
		Table table = HBaseUtil.getTable(tableName);
		//打印
		HBaseUtil.showScan(table, scan);
		//释放资源
		HBaseUtil.close(table);
	}


	public static void close(Admin admin) {
		try {
			if (admin != null) {
				admin.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void close(Table table) {
		try {
			if (table != null) {
				table.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
