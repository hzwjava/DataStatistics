package com.wochacha.da.transform.lyg;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;

import com.hadoop.compression.lzo.LzopCodec;
import com.wochacha.da.business.MainProcess;


public class LYGJob
{
    public boolean run(String inPath, String outPath, String strDayN)
    {
        JobClient client = new JobClient();
        JobConf conf = new JobConf(MainProcess.class);
        FileInputFormat.addInputPath(conf, new Path(inPath));
        conf.setInputFormat(TextInputFormat.class);
        conf.setMapperClass(LYGMapper.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setReducerClass(LYGReducer.class);
        FileOutputFormat.setOutputPath(conf, new Path(outPath));

        conf.setBoolean("mapred.output.compress", true);
        conf.setClass("mapred.output.compression.codec", LzopCodec.class, CompressionCodec.class);
        conf.set("mapred.output.compression.type", "BLOCK");

        conf.setJobName("LYG Job");
        client.setConf(conf);
        conf.set("job_date", strDayN);
        conf.setNumReduceTasks(30);
        try
        {
            JobClient.runJob(conf);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            return false;
        }
        return true;
    }
}
