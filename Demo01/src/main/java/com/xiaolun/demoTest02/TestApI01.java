package com.xiaolun.demoTest02;

import com.xiaolun.demoTest03.HBaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestApI01 {
	//配置文件信息
	public static Configuration conf;

	static {
		//使用 HBaseConfiguration 的单例方法实例化
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop101");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
	}

	//判断表是否存在
	public static boolean isTableExist(String tableName) throws Exception {
		//在 HBase 中管理、访问表需要先创建 HBaseAdmin 对象
		Connection connection = ConnectionFactory.createConnection(conf);

		//获取管理员对象
//        HBaseAdmin admin = new HBaseAdmin(conf); //过时方法
		HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
		boolean exist = admin.tableExists(tableName);

		//关闭连接
		admin.close();
		return exist;
	}

	//创建表（columnFamily 可变形参）
	public static void createTable(String tableName, String... columnFamily) throws Exception {
		HBaseAdmin admin = new HBaseAdmin(conf);
		//判断表是否存在
		if (isTableExist(tableName)) {
			System.out.println("表" + tableName + "已存在");
			//System.exit(0);
		} else {
			//创建表属性对象,表名需要转字节（创建表描述器）
			HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
			//循环创建多个列族
			for (String cf : columnFamily) {
				/**
				 * 1、new HColumnDescriptor(cf) 创建列族描述器
				 * 2、descriptor.addFamily()添加具体的列族信息
				 */
				descriptor.addFamily(new HColumnDescriptor(cf));
			}
			//根据对表的配置，创建表
			admin.createTable(descriptor);
			System.out.println("表" + tableName + "创建成功！");
		}
	}

	//删除表
	public static void dropTable(String tableName) throws Exception {
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (isTableExist(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
			System.out.println("表" + tableName + "删除成功！");
		} else {
			System.out.println("表" + tableName + "不存在！");
		}
	}

	//创建命名空间
	public static void createNameSpace(String ns) throws Exception {
		HBaseAdmin admin = new HBaseAdmin(conf);
		//1、创建命名空间
		NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(ns).build();

		try {
			admin.createNamespace(namespaceDescriptor);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 向表中插入数据
	 *
	 * @param tableName    表名
	 * @param rowKey       行键
	 * @param columnFamily 列族
	 * @param column       列
	 * @param value        值
	 * @throws Exception
	 */
	public static void addRowData(String tableName, String rowKey,
								  String columnFamily, String column, String value) throws Exception {
		//创建 HTable 对象
		HTable hTable = new HTable(conf, tableName);
		//向表中插入数据(字节数组)
		Put put = new Put(Bytes.toBytes(rowKey)); //？
		//向 Put 对象中组装数据
		put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
		hTable.put(put);
		hTable.close();
		System.out.println("插入数据成功");
	}

	/**
	 * 1、获取某一行数据 - get方法
	 * tableName 表名
	 * rowKey 行键
	 */
	public static void getRow(String tableName, String rowKey) throws Exception {
		HTable table = new HTable(conf, tableName);
		Get get = new Get(Bytes.toBytes(rowKey));
		//get.setMaxVersions();显示所有版本
		//get.setTimeStamp();显示指定时间戳的版本
		Result result = table.get(get);
		//解析result并打印
		for (Cell cell : result.rawCells()) {
			System.out.println(" 行 键 :" + Bytes.toString(result.getRow()));
			System.out.println(" 列 族 " + Bytes.toString(CellUtil.cloneFamily(cell)));
			System.out.println(" 列 :" + Bytes.toString(CellUtil.cloneQualifier(cell)));
			System.out.println(" 值 :" + Bytes.toString(CellUtil.cloneValue(cell)));
			System.out.println("时间戳:" + cell.getTimestamp());
		}
	}

	/**
	 * 获取某一行指定“列族: 列”的数据 - get
	 *
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param qualifier
	 * @throws Exception
	 */
	public static void getRowQualifier(String tableName, String rowKey,
									   String family, String qualifier) throws Exception {
		HTable table = new HTable(conf, tableName);
		Get get = new Get(Bytes.toBytes(rowKey));
		get.addColumn(Bytes.toBytes(family),
				Bytes.toBytes(qualifier));
		Result result = table.get(get);
		for (Cell cell : result.rawCells()) {
			System.out.println(" 行 键 :" +
					Bytes.toString(result.getRow()));
			System.out.println(" 列 族 " +
					Bytes.toString(CellUtil.cloneFamily(cell)));
			System.out.println(" 列 :" +
					Bytes.toString(CellUtil.cloneQualifier(cell)));
			System.out.println(" 值 :" +
					Bytes.toString(CellUtil.cloneValue(cell)));
		}
	}

	/**
	 * 获取全部数据 - scan
	 *
	 * @param tableName
	 * @throws Exception
	 */
	public static void getAllRows(String tableName) throws Exception {
		HTable hTable = new HTable(conf, tableName);
		//得到用于扫描 region 的对象
		Scan scan = new Scan();
		//使用 HTable 得到 resultcanner 实现类的对象
		ResultScanner resultScanner = hTable.getScanner(scan);
		for (Result result : resultScanner) {
			Cell[] cells = result.rawCells();
			for (Cell cell : cells) {
				//得到 rowkey
				System.out.println(" 行 键 :" + Bytes.toString(CellUtil.cloneRow(cell)));
				//得到列族
				System.out.println(" 列 族 " + Bytes.toString(CellUtil.cloneFamily(cell)));
				System.out.println(" 列 :" + Bytes.toString(CellUtil.cloneQualifier(cell)));
				System.out.println(" 值 :" + Bytes.toString(CellUtil.cloneValue(cell)));
			}
		}
	}

	/**
	 * 删除多行数据
	 *
	 * @param tableName
	 * @param rowsKey
	 */
	public static void deleteMultiRow(String tableName, String... rowsKey) throws Exception {
		HTable hTable = new HTable(conf, tableName);
		List<Delete> deleteList = new ArrayList<Delete>();
		//构建删除对象
		for (String row : rowsKey) {
			Delete delete = new Delete(Bytes.toBytes(row));
			deleteList.add(delete);
		}
		hTable.delete(deleteList);
		hTable.close();
	}

	/**
	 * @param tableName
	 * @desc modify table
	 */
	public static void alter(String tableName) {
		HBaseAdmin admin = HBaseUtil.getAdmin();
		//创建表的描述器
		try {
			HTableDescriptor tableDescriptor = admin.getTableDescriptor(TableName.valueOf(tableName));
			//添加列族
			HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("f1");
			tableDescriptor.addFamily(hColumnDescriptor);
			admin.modifyTable(tableName, tableDescriptor);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			HBaseUtil.close(admin);
		}
	}


	public static void main(String[] args) throws Exception {
		//1、测试表student01是否存在
//        System.out.println(isTableExist("student01"));

		//2、创建表测试
//        createTable("student02", "info1", "info2");
		//表示在nameSpace0408命名空间中创建表student02
//        createTable("nameSpace0408:student02", "info1", "info2");

		//3、删除表测试
//        dropTable("student02");

		//4、创建命名空间测试
//        createNameSpace("nameSpace0408");

		//5、向表中插入数据测试
//        addRowData("student01", "1001", "info", "name", "xiaohei");
//        addRowData("student01", "1002", "info", "age", "20");

		//6、获取某一行数据
//        getRow("student01", "1001");
//		getAllRows("student01");
		//获取某一行指定“列族: 列”的数据
//        getRowQualifier("student01","1001","info","name");

		//删除多行数据
//        deleteMultiRow("student01","1001");

		alter("fruit");
	}

}
