package com.xiaolun.demoTest02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class VersionTestDemo01 {
    //配置文件信息
    public static Configuration conf;

    static {
        //使用 HBaseConfiguration 的单例方法实例化
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop101");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
    }

    /**
     * @param tableName  表名
     * @param rowName    列键
     * @param familyName 列族
     * @param qualifier  修饰名
     */
    public static void getVersion(String tableName, byte[] rowName, String familyName, String qualifier) throws Exception {
        //new HTable(conf, tableName);推荐使用这一种方法
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));

        Scan scan = new Scan(rowName); //指定列族
        //设置一次性获取多少个版本的数据
        scan.setMaxVersions(3);
        scan.addColumn(familyName.getBytes(), qualifier.getBytes());
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.println(" 行 键 :" + Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.println(" 列 族 " + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println(" 列 :" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println(" 值 :" + Bytes.toString(CellUtil.cloneValue(cell)));
                System.out.println(" 时间戳 :" + cell.getTimestamp());
                System.out.println("-------------------------------------------------");
            }
        }

    }


    public static void main(String[] args) throws Exception {
        String rowName = "1002";
        getVersion("student01",rowName.getBytes(),"info","name");
    }
}
