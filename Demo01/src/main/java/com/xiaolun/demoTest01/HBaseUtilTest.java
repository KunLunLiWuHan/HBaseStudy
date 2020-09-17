package com.xiaolun.demoTest01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.After;
import org.junit.Before;


public class HBaseUtilTest {
	HBaseAdmin admin = null;

	@Before
	public void Before() throws Exception {
		//1、获取配置对象
		Configuration conf = new Configuration();
		//2、设置连接HBase的参数
		conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop101");
		//3、提取核心对象 admin
//		admin = new HBaseAdmin(conf); //过时
		admin = (HBaseAdmin) ConnectionFactory.createConnection(conf).getAdmin();
	}

	@After
	public void after() throws Exception {
		admin.close();
	}
}
