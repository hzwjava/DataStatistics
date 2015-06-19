package com.wochacha.da.transform.soft;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.wochacha.da.model.Record;

public class SoftWareMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text val, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {	
		String[] arr = val.toString().split("\t");
		String path = reporter.getInputSplit().toString();
		try {		
			if (path.indexOf("softFlag")>=0) { 					//常用软件统计UV和PV
				output.collect(new Text(arr[0]+"\t"+arr[3]+"\t"+arr[4]+"\t"+arr[5]), new Text(arr[2]));     // 0:标志  3:dist 4:os 5:ver 2:UDID
			}else if (path.indexOf("soft_DownloadFlag")>=0){	//下载软件
				output.collect(new Text(arr[0]+"\t"+arr[3]+"\t"+arr[4]+"\t"+arr[5]+"\t"+arr[6]), new Text(arr[2]));  //6:SFID
			}else if (path.indexOf("soft_TopicFlag")>=0){		//软件专题
				output.collect(new Text(arr[0]+"\t"+arr[3]+"\t"+arr[4]+"\t"+arr[5]+"\t"+arr[6]), new Text(arr[2]));  //6:SCID
			}
		} catch (Exception e) {
			output.collect(new Text("err_log"), new Text(val.toString()));
			// e.printStackTrace();
		}		
	}
}
