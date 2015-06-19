package com.wochacha.da.transform.scan;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;

import com.hadoop.compression.lzo.LzopCodec;
import com.wochacha.da.business.MainProcess;
import com.wochacha.da.model.Record;
import com.wochacha.da.util.DaMultiTextOutputFormat;

public class ScanJob {
	public boolean run(String inPath, String outPath, String strDayN){
		  JobClient client = new JobClient();
		  JobConf conf = new JobConf(MainProcess.class); 
//		  FileInputFormat.addInputPath(conf, new Path(inPath));
		  Configuration conf1 = new Configuration();
			FileSystem hdfs;
			try {
				hdfs = FileSystem.get(conf1);
				if (hdfs.exists(new Path(inPath))) {
					FileStatus[] stats = hdfs.listStatus(new Path(inPath));
					for (int i = 0; i < stats.length; i++) {
						if (stats[i].isDir()) {
							String fileName = stats[i].getPath().toString();
							if (fileName.indexOf("scan") >= 0 && fileName.indexOf("Flag") > 0){
								FileInputFormat.addInputPath(conf, stats[i].getPath());
							}
						}
					}
				}
			} catch (Exception e) {
				System.out.println(inPath+" is not exist!");
				return false;
			}
		  conf.setInputFormat(TextInputFormat.class);
		  conf.setMapperClass(ScanMapper.class);
		  conf.setOutputKeyClass(Text.class);
		  conf.setOutputValueClass(Text.class);
		  conf.setReducerClass(ScanReducer.class);	
		  conf.setOutputFormat(DaMultiTextOutputFormat.class);
		  conf.setJobName("ScanJob");
		  FileOutputFormat.setOutputPath(conf,new Path(outPath));
		  
		  conf.setBoolean("mapred.output.compress", true); 		
		  conf.setClass("mapred.output.compression.codec", LzopCodec.class,CompressionCodec.class);
		  conf.set("mapred.output.compression.type", "BLOCK");
		  
		  client.setConf(conf);
		  conf.set("job_date", strDayN);
//		  conf.setNumMapTasks(400);
		  conf.setNumReduceTasks(32);
		  try {
			  JobClient.runJob(conf);
		  } catch (Exception e) {
			  e.printStackTrace();
			  return false;
		  }
		  return true;
	}
}
