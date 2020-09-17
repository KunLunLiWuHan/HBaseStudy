package com.xiaolun.demoTest03;

import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

public class Demo01 {
	public static void main(String[] args) {
		System.out.println(isTableExist("fruit"));
	}

	public static boolean isTableExist(String tableName) {
		//获取管理员对象
		boolean exist = false;
		try {
			//执行到这里的时候，就是可以进入static中的方法，那么在其他类中就不用写conf方法了
			HBaseAdmin admin = HBaseUtil.getAdmin();
			exist = admin.tableExists(tableName);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return exist;
	}
}
