package com.wochacha.da.transform.express;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.omg.CORBA.INTERNAL;

import com.wochacha.da.dao.MysqlC3p0DAO;
import com.wochacha.da.dao.MysqlResultDao;
import com.wochacha.da.model.JobManager;
import com.wochacha.da.model.Record;
import com.wochacha.da.util.MsgSender;
import com.wochacha.da.extract.Query;
public class ExpressReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {
//	public static Connection conn_da = null;
    public static String job_date="";
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
//		if (conn_da == null) {
//			conn_da = JobManager.getResultDao();
//			if (conn_da == null) {
//				throw new IOException();
//			}
//		}
		String[] key_list = key.toString().split("\t");
		if (key_list[0].equalsIgnoreCase("chooseExpress")
				|| key_list[0].equalsIgnoreCase("scanExpress")
				|| key_list[0].equalsIgnoreCase("callExpress")
				|| key_list[0].equalsIgnoreCase("markExpress")) {
			FUN_ExpressIdResultToDB("DA_ExpressTop_Analysis",values,key.toString(), output);
			
		} else if(!key_list[0].equalsIgnoreCase("error_log")){
			FUN_ExpressResultToDB("DA_ExpressItem_Analysis",values,key.toString(), output);
		}		
	}
	 public void FUN_ExpressResultToDB(String tableName,Iterator<Text> values,String key,OutputCollector<Text, Text> output){
			String Date = job_date;
			String UDID = "";
			String OS = "";
			String Ver="";
			String CTID = "";
			String FunType = "";
		    int PV = 0;  //访问量	 
	    	int UV = 0;	 //访问用户数
	    	HashSet<String> udid_set = new HashSet<String>();
	    	String[] key_list = key.split("\t");  
	    	if(key_list.length==4){
	    		FunType = key_list[0];
				OS = key_list[1];
				Ver=key_list[2];
				CTID = key_list[3];    			
		    	while (values.hasNext()) {
					String[] value = values.next().toString().split("\t");
					UDID = value[0];
					udid_set.add(UDID);
					PV++;
				}
				UV = udid_set.size();  			
				String[] volConName = { "Date","OS","Ver","CTID","FunType"};
				String[] volConValue = { Date,OS,Ver,CTID,FunType};
				String[] volNameInsert = { "Date","OS","Ver","CTID","FunType","UV","PV"};
				String[] volValueInsert = { Date,OS,Ver,CTID,FunType,UV+"",PV+""};
				String[] VName = { "UV","PV"};
				String[] VValue = { UV+"",PV+""};	
				try {
//					JobManager.ResultToDB(conn_da, tableName, volConName, volConValue, volNameInsert, volValueInsert, VName, VValue);
					JobManager.ResultToDB(output, tableName, volConName, volConValue, volNameInsert, volValueInsert, VName, VValue,"");
//				} catch (SQLException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//					MsgSender.sendMessage(new String[] {"err","Insert error\t"+job_date+"\tExposureReducer"+FunType+"\t"+e.getMessage()});
				}catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					MsgSender.sendMessage(new String[] {"err","Insert error\t"+job_date+"\tExpressReducer"+FunType+"\t"+e.getMessage()});
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					MsgSender.sendMessage(new String[] {"err","Insert error\t"+job_date+"\tExposureReducer"+FunType+"\t"+e.getMessage()});
				}
	    	}	    	
	}
	 public void FUN_ExpressIdResultToDB(String tableName,Iterator<Text> values,String key,OutputCollector<Text, Text> output){
	    	int PV = 0;  //访问量	 
	    	int UV = 0;	 //访问用户数
	    	int PV_y = 0;  //扫描快递-有结果
	    	int PV_n = 0; //扫描快递-无结果
	    	int PV_e = 0; //扫描快递-不匹配
	    	String Date=job_date;
	    	String UDID = "";
	    	String OS = "";
	    	String Ver = "";
	     	String CTID = "";
			String FunType = "";
			String ObjectId = "";
			String result = "";
	    	HashSet<String> udid_set = new HashSet<String>();
	    	String[] key_list = key.split("\t");     
	    	if(key_list.length==5){
				FunType = key_list[0];
		    	OS = key_list[1];
		    	Ver = key_list[2];
				CTID = key_list[3]; 
		    	ObjectId = key_list[4];
		    	while (values.hasNext()) {
					String[] value = values.next().toString().split("\t");									
					UDID = value[0];
					udid_set.add(UDID);
					PV++;
					
					if(FunType.equals("scanExpress")){
						if(value.length==2){
							result=value[1];
							if(result.equals("y")){
								PV_y++;
							}else if(result.equals("n")){
								PV_n++;
							}else{
								PV_e++;
							}
						}
					}	
				}
				UV = udid_set.size();  	
				String[] volConName = { "Date","OS","Ver","CTID","FunType","ObjectId"};
				String[] volConValue = { Date,OS,Ver,CTID,FunType+"",ObjectId};
				String[] volNameInsert = { "Date","OS","Ver","CTID","FunType","ObjectId","UV","PV","PV_y","PV_n","PV_e"};
				String[] volValueInsert = { Date,OS,Ver,CTID,FunType+"",ObjectId,UV+"",PV+"",PV_y+"",PV_n+"",PV_e+""};
				String[] VName = { "UV","PV","PV_y","PV_n","PV_e"};
				String[] VValue = { UV+"",PV+"",PV_y+"",PV_n+"",PV_e+""};	
				try {
//					JobManager.ResultToDB(conn_da, tableName, volConName, volConValue, volNameInsert, volValueInsert, VName, VValue);			
					JobManager.ResultToDB(output, tableName, volConName, volConValue, volNameInsert, volValueInsert, VName, VValue,"");
//				} catch (SQLException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//					MsgSender.sendMessage(new String[] {"err","Insert error\t"+job_date+"\tExposureReducer"+FunType+"\t"+e.getMessage()});
				}catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					MsgSender.sendMessage(new String[] {"err","Insert error\t"+job_date+"\tExpressReducer"+FunType+"\t"+e.getMessage()});
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					MsgSender.sendMessage(new String[] {"err","Insert error\t"+job_date+"\tExposureReducer"+FunType+"\t"+e.getMessage()});
				}			
	    	}	    	
	 }

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
//		if (conn_da != null) {
//			try {
//				conn_da.close();
//				conn_da = null;
//			} catch (Exception e) {
//
//			}
//			conn_da = null;
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
