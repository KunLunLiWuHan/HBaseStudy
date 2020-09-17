package com.xiaolun;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

/**
 * 代码能够跑通，
 * 可以将HDFS中的数据放到HBase中存储
 * 但是存的结果不对。
 * 需要重新调试
 */
public class HDFS2HBase implements Tool {

	private Configuration conf;

	@Override
	public int run(String[] args) throws Exception {
		//1、获取job
		Job job = Job.getInstance(conf);
		//2、设置jar 写上驱动的名字
		job.setJarByClass(HDFS2HBase.class);
		//3、设置其他
		job.setMapperClass(HBaseMapper2.class);
		job.setReducerClass(HBaseReducer2.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		TableMapReduceUtil.initTableReducerJob("t3", HBaseReducer2.class, job);
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
	public static class HBaseMapper2 extends Mapper<LongWritable, Text, Text, LongWritable> {
		private Text mk = new Text();
		private LongWritable mv = new LongWritable(1);

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//1、读取一行
			String line = value.toString();
			//2、切分
			String[] columns = line.split(",");
			//3、遍历
			for (String column : columns) {
				mk.set(column);
				context.write(mk, mv);
			}
		}
	}

	//TableReducer<KEYIN, VALUEIN, KEYOUT> extends Reducer<KEYIN, VALUEIN, KEYOUT, Mutation>
	public static class HBaseReducer2 extends TableReducer<Text, LongWritable, ImmutableBytesWritable> {
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			//1、计数器
			long count = 0;
			Iterator<LongWritable> iterator = values.iterator();
			while (iterator.hasNext()) {
				LongWritable next = iterator.next();
				count += next.get();
			}
			//2、行键
			Put put = new Put(Bytes.toBytes(key.toString()));
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(key.toString()));
//			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("count"), Bytes.toBytes(count));
			//3、写回
			context.write(new ImmutableBytesWritable(Bytes.toBytes(key.toString())), put);
		}
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(HBaseConfiguration.create(), new HDFS2HBase(), args);
	}
}
