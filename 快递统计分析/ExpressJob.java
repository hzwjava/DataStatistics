package com.wochacha.da.transform.express;
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

public class ExpressJob {
	public boolean run(String inPath, String outPath, String strDayN){
		  JobClient client = new JobClient();
		  JobConf conf = new JobConf(MainProcess.class); 
		  FileInputFormat.addInputPath(conf, new Path(inPath));		  
//		  FileInputFormat.setInputPaths(conf, new Path(inPath));
		  conf.setInputFormat(TextInputFormat.class);
		  conf.setMapperClass(ExpressMapper.class);
		  conf.setOutputKeyClass(Text.class);
		  conf.setOutputValueClass(Text.class);
		  conf.setReducerClass(ExpressReducer.class);	
		  FileOutputFormat.setOutputPath(conf,new Path(outPath));
		  
		  conf.setBoolean("mapred.output.compress", true); 		
		  conf.setClass("mapred.output.compression.codec", LzopCodec.class,CompressionCodec.class);
		  conf.set("mapred.output.compression.type", "BLOCK");
		  
		  conf.setJobName("ExpressJob");
		  client.setConf(conf);
		  conf.set("job_date", strDayN);
		  conf.setNumReduceTasks(30);
		  try {
			  JobClient.runJob(conf);
		  } catch (Exception e) {
			  e.printStackTrace();
			  return false;
		  }
		  return true;
	}
}
