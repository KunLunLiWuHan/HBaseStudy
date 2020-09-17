import org.apache.hadoop.hbase.util.Bytes;

public class Test {
    public static void main(String[] args) {
        byte[] bytes = Bytes.toBytes(18);
        byte[] bytes2 = Bytes.toBytes("18");
        System.out.println(bytes == bytes2);
        for (byte aByte : bytes) {
            System.out.println(aByte);
        }
        System.out.println("---------------------------");
        for (byte aByte : bytes2) {
            System.out.println(aByte);
        }
    }
}
