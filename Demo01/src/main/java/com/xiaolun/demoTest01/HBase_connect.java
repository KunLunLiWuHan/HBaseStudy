package com.xiaolun.demoTest01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

//测试连接
public class HBase_connect {
	public static void main(String[] args) {
		//1、获取配置对象
		Configuration conf = new Configuration();
		//2、设置连接HBase的参数
		conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop101");

		//3、获取核心对象Admin
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(conf);
			//4、通过Admin来测试（表fruit是否存在）
			boolean exists = admin.tableExists("fruit");
			System.out.println("exists --> "+ exists); //exists --> true
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			try {
				admin.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
