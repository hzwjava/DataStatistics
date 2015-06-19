package com.wochacha.da.util;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseUtil {

	// 禁用表
	public void disableTable(HBaseAdmin admin, String table) {
		try {
			admin.disableTable(table);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// 删除表
	public void dropTable(HBaseAdmin admin, String tableName) {
		if (existsTable(admin, tableName)) {
			disableTable(admin, tableName);
			try {
				admin.deleteTable(tableName);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	// 判定表是否存在
	public static boolean existsTable(HBaseAdmin admin, String tableName) {
		try {
			return admin.tableExists(tableName.getBytes());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}

	// 创建数据库=表
	public static void createTable(HBaseAdmin ha, String tableName, String cf) {
		HTableDescriptor hd = new HTableDescriptor(tableName);
		HColumnDescriptor hc = new HColumnDescriptor(cf);
		// hc.setMaxVersions(10);
		hd.addFamily(hc);
		try {
			ha.createTable(hd);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static Result getValues(HTableInterface htable, String rowkey,
			String cf) {
		Result result = null;
		Get get = new Get(rowkey.getBytes());
		get.addFamily(cf.getBytes());
		try {
			result = htable.get(get);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;
	}

	// 插入hbase中获得数据，傳入表名tableName,行键rowkey,列族cf,列名column,值value.
	public static String getValue(HTableInterface htable, String rowkey,
			String cf, String column) {
		Get get = new Get(rowkey.getBytes());
		get.addColumn(cf.getBytes(), column.getBytes());
		String createTime = null;
		try {
			Result result = htable.get(get);
			if (result.value() != null) {
				createTime = new String(result.value());
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return createTime;
	}

	// 插入數據到Hbase中，傳入表名tableName,行键rowkey,列族cf,列名column,值value.
	public static void putToHBase(HTableInterface htable, String rowkey,
			String cf, String column, String value) {

		Put put = new Put(rowkey.getBytes());
		put.add(cf.getBytes(), column.getBytes(), value.getBytes());
		try {
			htable.put(put);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// 插入數據到Hbase中，傳入表名tableName,行键rowkey,列族cf,列名column,值value.（指定时间戳）
	public static void putOVC(HTableInterface htable, String rowkey, String cf,
			String column, int value, String day) {
		Long timeStamp = DateFormatUtil.formatStringTimeToLong(day);// 指定插入的时间戳
		Put put = new Put(Bytes.toBytes(rowkey));
		put.add(Bytes.toBytes(cf), Bytes.toBytes(column), timeStamp,
				Bytes.toBytes(value + ""));
		try {
			htable.put(put);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void scanOVC(HTableInterface htable, String cf, String colum,
			String day, HashMap<String, Integer> sumDevice_map)
			throws IOException {
		Long timeStamp = DateFormatUtil.formatStringTimeToLong(day) - 86400000;
		Scan scan = new Scan();
		scan.setTimeStamp(timeStamp);
		scan.addColumn(Bytes.toBytes(cf), Bytes.toBytes(colum));
		ResultScanner scanner = htable.getScanner(scan);
		for (Result res : scanner) {
			KeyValue[] keyvalue = res.raw();
			for (KeyValue kv : keyvalue) {
				sumDevice_map.put(new String(kv.getRow()),
						Integer.parseInt(new String(kv.getValue())));
			}
		}
		scanner.close();
	}
	public static String queryIp(HTableInterface ip_hti, String ip)
			throws IOException { // 根据IP地址去IP表查询country_Provinces_cities_Operators
		String country = "unknown";
		String Provinces = "unknown";
		String cities = "unknown";
		String Operators = "unknown";
		if (ip.startsWith("10.") || ip.startsWith("192.168.")) {
			return "局域网\t局域网\t局域网\t局域网";
		}
		Get get = new Get(ip.getBytes());
		Result result = ip_hti.get(get);
		if (result != null && !result.isEmpty()) {
			Cell cell1 = result.getColumnLatestCell(INFO_CF,
					"Country".getBytes());
			Cell cell2 = result.getColumnLatestCell(INFO_CF,
					"Provinces".getBytes());
			Cell cell3 = result.getColumnLatestCell(INFO_CF,
					"Cities".getBytes());
			Cell cell4 = result.getColumnLatestCell(INFO_CF,
					"Operators".getBytes());
			if (cell1 != null) {
				byte[] dist_org_byte = CellUtil.cloneValue(cell1);
				country = new String(dist_org_byte);
			}
			if (cell2 != null) {
				byte[] dist_last_byte = CellUtil.cloneValue(cell2);
				Provinces = new String(dist_last_byte);
			}
			if (cell3 != null) {
				byte[] created_time_byte = CellUtil.cloneValue(cell3);
				cities = new String(created_time_byte);
			}
			if (cell4 != null) {
				byte[] model_byte = CellUtil.cloneValue(cell4);
				Operators = new String(model_byte);
			}
		}
		if (country.indexOf("中国") < 0 && country.indexOf("unknown") < 0) {
			Provinces = "国外";
			cities = "国外";
			Operators = "国外";
		}

		country = country.trim();
		Provinces = Provinces.trim();
		cities = cities.trim();
		if (cities.indexOf("（") >= 0) {
			cities = cities.substring(0, cities.indexOf("（"));
		}
		if (cities.indexOf("/") >= 0) {
			cities = cities.substring(0, cities.indexOf("/"));
		}
		Operators = Operators.trim();
		if (Operators.equals("局域网")) {
			return "局域网\t局域网\t局域网\t局域网";
		}
		return country + "\t" + Provinces + "\t" + cities + "\t" + Operators;
	}
}
