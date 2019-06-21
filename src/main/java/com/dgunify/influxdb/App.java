package com.dgunify.influxdb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Result;
import org.influxdb.dto.QueryResult.Series;

/**
 * Hello world!
 *
 */
public class App 
{
	static InfluxDBConnection influxDBConnection = new InfluxDBConnection("admin", "admin", "http://10.1.38.254:8954", "lt_system2", "");
    public static void query()
    {
		//QueryResult results = influxDBConnection.query("SELECT * FROM tb_zdg where name = '大脑补丁'  order by time desc limit 1000");
		QueryResult results = influxDBConnection.query("SELECT * FROM tb_zdg order by time desc limit 10");
		//results.getResults()是同时查询多条SQL语句的返回值，此处我们只有一条SQL，所以只取第一个结果集即可。
		Result oneResult = results.getResults().get(0);
		if (oneResult.getSeries() != null) {
			List<List<Object>> valueList = oneResult.getSeries().stream().map(Series::getValues)
					.collect(Collectors.toList()).get(0);
			
			if (valueList != null && valueList.size() > 0) {
				System.out.println(valueList.size());
				System.exit(1);
				for (List<Object> value : valueList) {
					Map<String, String> map = new HashMap<String, String>();
					// 数据库中字段1取值
					String field1 = value.get(0) == null ? null : value.get(0).toString();
					// 数据库中字段2取值
					String field2 = value.get(1) == null ? null : value.get(1).toString();
					System.out.println(field1+"========"+field2);
				}
			}
		}
		
		/**
		 * 取数据的时候，注意空值判断，本例将返回数据先进行判空oneResult.getSeries() != null，然后调用oneResult.getSeries().getValues().get(0)获取到第一条SQL的返回结果集，然后遍历valueList，取出每条记录中的目标字段值。
		 */
    }
    
    
    public static void insert()
    {
    	
		Map<String, String> tags = new HashMap<String, String>();
		tags.put("tag1", "标签值");
		Map<String, Object> fields = new HashMap<String, Object>();
		fields.put("field1", "String类型");
		// 数值型，InfluxDB的字段类型，由第一天插入的值得类型决定
		fields.put("field2", 3.141592657);
		// 时间使用毫秒为单位
		influxDBConnection.insert("tb_zdg", tags, fields, System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }
    
    public static void forinsert( String[] args )
    {
    	
		Map<String, String> tags = new HashMap<String, String>();
		tags.put("tag1", "标签值");
		Map<String, Object> fields1 = new HashMap<String, Object>();
		fields1.put("field1", "abc");
		// 数值型，InfluxDB的字段类型，由第一天插入的值得类型决定
		fields1.put("field2", 123456);
		Map<String, Object> fields2 = new HashMap<String, Object>();
		fields2.put("field1", "String类型");
		fields2.put("field2", 3.141592657);
		// 一条记录值
		Point point1 = influxDBConnection.pointBuilder("表名", System.currentTimeMillis(), tags, fields1);
		Point point2 = influxDBConnection.pointBuilder("表名", System.currentTimeMillis(), tags, fields2);
		// 将两条记录添加到batchPoints中
		BatchPoints batchPoints1 = BatchPoints.database("db-test").tag("tag1", "标签值1").retentionPolicy("hour")
				.consistency(ConsistencyLevel.ALL).build();
		BatchPoints batchPoints2 = BatchPoints.database("db-test").tag("tag2", "标签值2").retentionPolicy("hour")
				.consistency(ConsistencyLevel.ALL).build();
		batchPoints1.point(point1);
		batchPoints2.point(point2);
		// 将两条数据批量插入到数据库中
		influxDBConnection.batchInsert(batchPoints1);
		influxDBConnection.batchInsert(batchPoints2);
    }
    
    ////推荐使用第二种方式，属于一个数据库的数据，可以一次性批量写入，写入速度最快
    public static void insertall()
    {
		
	
		// 将不同的batchPoints序列化后，一次性写入数据库，提高写入速度
		List<String> records = new ArrayList<String>();
		
		
		for(int i=0;i<50000;i++) {
			Map<String, String> tags1 = new HashMap<String, String>();
			tags1.put("tag", i+"");
			Map<String, Object> fields1 = new HashMap<String, Object>();
			fields1.put("name", "n_m_"+i);
			// 数值型，InfluxDB的字段类型，由第一天插入的值得类型决定
			fields1.put("age", i);
			fields1.put("sex", new Random().nextInt(2));
			fields1.put("num", new Random().nextInt(9999));
			
			// 一条记录值
			Point point1 = influxDBConnection.pointBuilder("tb_bigdata", System.currentTimeMillis(), tags1, fields1);
			BatchPoints batchPoints1 = BatchPoints.database("lt_system2")
					.retentionPolicy("autogen").consistency(ConsistencyLevel.ALL).build();
			batchPoints1.point(point1);
			records.add(batchPoints1.lineProtocol());
		}
		
		//System.out.println("ts:"+records.size());
		// 将两条数据批量插入到数据库中
		influxDBConnection.batchInsert("lt_system2", "", ConsistencyLevel.ALL, records);
    }
    public static void main(String[] args) throws InterruptedException {
    	//query();
		/*
		 * for(int i=0;i<10;i++) { insertall(); System.out.println("00");
		 * Thread.sleep(4000); }
		 */
    	//System.out.println();
    	for(int i=0;i<150;i++) {
    		System.out.println(i);
    		insertall();
    		Thread.sleep(4000);
    	}
	}
    
}
