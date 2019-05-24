package spark.streaming;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseTableUtils {
	private static Configuration getConfiguration() {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop:2181");
		conf.set("zookeeper.znode.parent", "/hbase");
		return conf;
	}
	
	static Table createHbaseTable(String tableName, String familiarName)
		throws Exception {
		Configuration conf = getConfiguration();
		Connection connection = ConnectionFactory.createConnection(conf);
		Admin admin = connection.getAdmin();
		
		if (!admin.tableExists(TableName.valueOf(tableName))) {
			HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
			HColumnDescriptor columnDescriptor = new HColumnDescriptor(Bytes.toBytes(familiarName));
			tableDescriptor.addFamily(columnDescriptor);
			admin.createTable(tableDescriptor);
		}
		return connection.getTable(TableName.valueOf(tableName));
	}
	
}
