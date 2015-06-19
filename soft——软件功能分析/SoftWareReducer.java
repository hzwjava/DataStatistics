package com.wochacha.da.transform.soft;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import com.wochacha.da.dao.MysqlC3p0DAO;
import com.wochacha.da.dao.MysqlResultDao;
import com.wochacha.da.model.JobManager;
import com.wochacha.da.model.Record;
import com.wochacha.da.util.MsgSender;
import com.wochacha.da.extract.Query;;
public class SoftWareReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {   
//	public static Connection conn_230 = null;
	public String job_date = "";
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
//    	 if(conn_230 == null){
//    		 conn_230 =  JobManager.getResultDao();
//    		 if(conn_230 == null){
//    			 throw new IOException();  			 
//    		 }
//		 }      	
     	String Date = job_date;
     	String OS = "";
     	String Ver = "";
     	String Channel = "";
     	String SFID = "";
     	String sfAD = "";
     	String[] key_list = key.toString().split("\t");     	
     	if(key_list[0].equals("softFlag")){			// 常用软件
        	long SoftPV = 0;	  //软件访问量
        	long SoftUV = 0;	  //软件用户量
     		HashSet<String> udid_set = new HashSet<String>();
     		if (key_list.length == 4) {
    			Channel = key_list[1];
    			OS = key_list[2];
    			Ver = key_list[3];    			
    			while (values.hasNext()) {
    				String[] value = values.next().toString().split("\t");
    				udid_set.add(value[0]);
    				SoftPV++;
    			}
    			SoftUV = udid_set.size();    			
    			String[] volName = { "Date", "OS", "Ver", "Channel", "SoftUV", "SoftPV"};
    			String[] volValue = { Date, OS, Ver, Channel, SoftUV+"", SoftPV+"" };
    			String[] volConName = { "Date", "OS", "Ver", "Channel" };
    			String[] volConValue = { Date, OS, Ver, Channel };
    			String[] Vname = new String[] { "SoftUV", "SoftPV" };
    			String[] Vvalue = new String[] {SoftUV+"", SoftPV+""};    			
    			try {
//					JobManager.ResultToDB(conn_230, "DA_Soft_Analysis", volConName, volConValue, volName, volValue, Vname, Vvalue);
    				JobManager.ResultToDB(output, "DA_Soft_Analysis", volConName, volConValue, volName, volValue, Vname, Vvalue,"");
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					MsgSender.sendMessage(new String[] {"err","Insert error\t"+Date+"\tSoftReducer"+key_list[0]+"\t"+e.getMessage()});
				}
    		}
     	}else if(key_list[0].equals("soft_DownloadFlag")){
     		long DownPV = 0;
     		long DownUV = 0;
     		if (key_list.length == 5) {
    			Channel = key_list[1];
    			OS = key_list[2];
    			Ver = key_list[3];
    			SFID = key_list[4];
    			HashSet<String> downudid_set = new HashSet<String>();
    			while (values.hasNext()) {
    				String[] value = values.next().toString().split("\t");
    				downudid_set.add(value[0]);
    				DownPV++;    				 				
    			}
    			DownUV = downudid_set.size();
    			String[] volName = { "Date", "OS", "Ver", "Channel", "SFID", "DownUV", "DownPV" };
    			String[] volValue = { Date, OS, Ver, Channel, SFID, DownUV+"", DownPV+""};
    			String[] volConName = { "Date", "OS", "Ver", "Channel", "SFID" };
				String[] volConValue = { Date, OS, Ver, Channel, SFID };
				String[] Vname = new String[] { "DownUV", "DownPV" };
				String[] Vvalue = new String[] { DownUV + "", DownPV + "" };
    			// 功能-软件详情
    			try {
//					JobManager.ResultToDB(conn_230, "DA_SoftDown_Analysis", volConName, volConValue, volName, volValue, Vname, Vvalue);
					JobManager.ResultToDB(output, "DA_SoftDown_Analysis", volConName, volConValue, volName, volValue, Vname, Vvalue,"");
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					MsgSender.sendMessage(new String[] {"err","Insert error\t"+Date+"\tSoftWareReducerSoftReducer"+key_list[0]+"\t"+e.getMessage()});
				}
     		}
     	}else if(key_list[0].equals("soft_TopicFlag")){
     		long TopicPV = 0;
     		long TopicUV = 0;
     		String SCID = "";
     		HashSet<String> downudid_set = new HashSet<String>();
     		if (key_list.length == 5) {
    			Channel = key_list[1];
    			OS = key_list[2];
    			Ver = key_list[3];
    			SCID = key_list[4];
    			
    			while (values.hasNext()) {
    				String[] value = values.next().toString().split("\t");
    				downudid_set.add(value[0]);
    				TopicPV++;    				 				
    			}
    			TopicUV = downudid_set.size();
    			String[] volName = { "Date", "OS", "Ver", "Channel", "SCID", "TopicUV", "TopicPV" };
    			String[] volValue = { Date,OS,Ver,Channel,SCID, TopicUV+"", TopicPV+""};
    			String[] volConName = { "Date", "OS", "Ver", "Channel", "SCID" };
				String[] volConValue = { Date, OS, Ver, Channel, SCID };
				String[] Vname = new String[] { "TopicUV", "TopicPV" };
				String[] Vvalue = new String[] { TopicUV + "", TopicPV + "" };
    			// 功能-软件专题
    			try {
//					JobManager.ResultToDB(conn_230, "DA_SoftTopic_Analysis", volConName, volConValue, volName, volValue, Vname, Vvalue);
    				JobManager.ResultToDB(output, "DA_SoftTopic_Analysis", volConName, volConValue, volName, volValue, Vname, Vvalue,"");
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					MsgSender.sendMessage(new String[] {"err","Insert error\t"+Date+"\tSoftWareReducerSoftReducer"+key_list[0]+"\t"+e.getMessage()});
				}
     		}
     	}
    }
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
//		if(conn_230 != null){
//			try{
//				conn_230.close();
//				conn_230 = null;
//			}catch(Exception e){
//				
//			}
//			conn_230 = null;			
//		}
		super.close();
	}
	@Override
	public void configure(JobConf job) {
		// TODO Auto-generated method stub
		super.configure(job);
		job_date=job.get("job_date");
	}
}
