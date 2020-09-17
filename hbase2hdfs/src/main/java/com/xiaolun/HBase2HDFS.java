package com.xiaolun;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class HBase2HDFS implements Tool {

	private Configuration conf;

	@Override
	public int run(String[] args) throws Exception {
		//1、获取job
		Job job = Job.getInstance(conf);
		//2、设置jar 写上驱动的名字
		job.setJarByClass(HBase2HDFS.class);
		//3、设置输入输出 和原来的mapreduce不同
		TableMapReduceUtil.initTableMapperJob("t2",
				new Scan(), HBaseMapper.class,
				Text.class, NullWritable.class, job);
		FileOutputFormat.setOutputPath(job, new Path(args[0]));
		//4、提交
		return job.waitForCompletion(true) ? 1 : 0;
	}

	@Override
	public void setConf(Configuration conf) {
		//1、配置连接HBase的路径
		conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop101");
		//2、配置连接HDFS
		conf.set("fs.defaultFS", "hdfs://hadoop101:9000");
		//3、配置连接mapreduce
		conf.set("mapreduce.framework.name", "yarn");
		//configuration保持一致
		this.conf = conf;
	}

	@Override
	public Configuration getConf() {
		//获取configuration
		return conf;
	}

	/**
	 * 1、TableMapper<KEYOUT, VALUEOUT>
	 * 2、从HBase上输出数据到HDFS(不要数据，只要一个文本)
	 */
	public static class HBaseMapper extends TableMapper<Text, NullWritable> {
		private Text mk = new Text();

		@Override
		protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
			//1、创建字符串对象
			StringBuffer stringBuffer = new StringBuffer();
			//2、获取扫描
			CellScanner cellScanner = value.cellScanner();
			//3、扫描
			//遍历扫描器（没有try-catch包围也没有报错）
			while (cellScanner.advance()) {
				//4、获取一个表格
				Cell cell = cellScanner.current();
				//5、按逗号进行拼接
				stringBuffer.append(new String(CellUtil.cloneValue(cell), "utf-8")).append(",");
			}
			//6、设置key
			mk.set(stringBuffer.toString());
			//7、写出
			context.write(mk, NullWritable.get());
		}
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(HBaseConfiguration.create(), new HBase2HDFS(), args);
	}
}
