package com.wochacha.da.transform.scan;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;

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
public class ScanReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {   
//	public static Connection conn_230 = null;
	public String job_date = "";
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
//    	 if(conn_230 == null){
//    		 conn_230 =  JobManager.getResultDao();
//    		 if(conn_230 == null){
//    			 throw new IOException();  			 
//    		 }
//		 } 
    	long ScanDevNum = 0;   //扫描设备数
    	long ScanNum = 0;      //扫描次数
    	long ScanTypeUV = 0;
    	long ScanTypePV = 0;
    	String ScanType = "";
    	String Date = job_date;
    	String OS = "";
    	String Ver = "";
    	String Channel = "";
    	String ProductType = "";
    	String[] key_list = key.toString().split("\t");
    	HashSet<String> ScanDev_set = new HashSet<String>();
    	if(key_list[0].equals("User_User_analysis")){
    		if(key_list.length == 5){
            	Channel = key_list[1];
            	OS = key_list[2];
            	Ver = key_list[3];
            	ProductType = key_list[4];
            }
        	while(values.hasNext()){
        		String value = values.next().toString();
        		ScanDev_set.add(value);                        //扫描设备
        		ScanNum ++;                                    //扫描次数
        	}   
        	ScanDevNum = ScanDev_set.size(); 
        	String[] volName = {"Date", "OS", "Ver", "Channel","ProductType", "ScanDevNum", "ScanNum"};
        	String[] volValue = {Date,OS, Ver, Channel,ProductType, ScanDevNum+"", ScanNum+""};
        	String[] volConName = {"Date", "OS", "Ver", "Channel","ProductType"};
        	String[] volConValue = {Date, OS, Ver, Channel,ProductType};
        	String[] Vname = new String[]{"ScanDevNum", "ScanNum"};
        	String[] Vvalue = new String[]{ScanDevNum+"",ScanNum+""};  
        	//User
            try {
//				JobManager.ResultToDB(conn_230, "DA_User_Analysis", volConName, volConValue, volName, volValue, Vname, Vvalue);
				JobManager.ResultToDB(output, "DA_User_Analysis", volConName, volConValue, volName, volValue, Vname, Vvalue,"");
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				MsgSender.sendMessage(new String[] {"err","Insert error\t"+Date+"\tScanReducer\t"+e.getMessage()});
			}  
            FUN_ScanResultToDB("DA_Scan_Analysis","Scan",ScanDevNum,ScanNum, Date, OS, Ver, Channel,ProductType, output);  
//            output.collect(new Text(key.toString()), new Text(ScanDevNum+"\t"+ScanNum));  
    	}else if(key_list[0].equals("UsrRegion_analysis")){
    		String ProvinceCode = "";
    		String CityCode = "";
    		String operator="";
    		String[] CTID;
    		if(key_list.length == 7){
    			CTID = key_list[1].split("_");	
            	Channel = key_list[2];
            	OS = key_list[3];
            	Ver = key_list[4];
            	ProvinceCode = CTID[0];
            	CityCode = CTID[1];
            	ProductType = key_list[5];
            	operator=key_list[6];
            }        	
        	while(values.hasNext()){
        		String value = values.next().toString();
//        		ScanDevNum++;                                     //扫描设备数
        		ScanNum ++;                                       //扫描次数
        	}    	
        	String[] volName = {"Date", "OS", "Ver", "Channel","ProductType" ,"ProvinceCode","CityCode","ScanNum","operator"};
        	String[] volValue = {Date, OS, Ver, Channel,ProductType, ProvinceCode, CityCode, ScanNum+"",operator};
        	String[] volConName = {"Date", "OS", "Ver", "Channel","ProductType" ,"ProvinceCode","CityCode","operator"};
        	String[] volConValue = {Date, OS, Ver, Channel, ProductType,ProvinceCode, CityCode,operator};
        	String[] Vname = new String[]{"ScanNum"};
        	String[] Vvalue = new String[]{ScanNum+""};  
        	//用户地区分析
        	try {
//				JobManager.ResultToDB(conn_230, "DA_UsrRegion_Analysis", volConName, volConValue, volName, volValue, Vname, Vvalue);
				JobManager.ResultToDB(output, "DA_UsrRegion_Analysis", volConName, volConValue, volName, volValue, Vname, Vvalue,"");
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				MsgSender.sendMessage(new String[] {"err","Insert error\t"+Date+"\tScanReducer\t"+e.getMessage()});
			}
    	}else if(key_list[0].equals("scanBarcodeFlag")){    		
    		HashSet<String> Barcode_set = new HashSet<String>();
    		ScanType = "Barcode";
    		if(key_list.length == 5){
    			Channel = key_list[1];
            	OS = key_list[2];
            	Ver = key_list[3];
            	ProductType = key_list[4];
    		}
    		while(values.hasNext()){
    			String value = values.next().toString();
    			Barcode_set.add(value);                           //条形码扫描设备
        		ScanTypePV ++;                                    //条形码扫描次数
    		}
    		ScanTypeUV = Barcode_set.size();
    		FUN_ScanResultToDB("DA_Scan_Analysis",ScanType,ScanTypeUV,ScanTypePV, Date, OS, Ver, Channel,ProductType, output);   
//    		output.collect(new Text(key.toString()), new Text(ScanTypeUV+"\t"+ScanTypePV));  
    	}else if(key_list[0].equals("scan2DCode")){
    		HashSet<String> set_2DCode = new HashSet<String>();
    		ScanType = "TDCode";
    		if(key_list.length == 5){
    			Channel = key_list[1];
            	OS = key_list[2];
            	Ver = key_list[3];
            	ProductType = key_list[4];
    		}
    		while(values.hasNext()){
    			String value = values.next().toString();
    			set_2DCode.add(value);                            //二维码扫描设备
        		ScanTypePV ++;                                    //二维码扫描次数
    		}
    		ScanTypeUV = set_2DCode.size();
    		FUN_ScanResultToDB("DA_Scan_Analysis",ScanType,ScanTypeUV,ScanTypePV, Date, OS, Ver, Channel,ProductType, output);
//    		output.collect(new Text(key.toString()), new Text(ScanTypeUV+"\t"+ScanTypePV));
    	}else if(key_list[0].equals("scan2DCode_QRCodeFlag")){
    		HashSet<String> set_QRCode = new HashSet<String>();
    		ScanType = "QRCode";
    		if(key_list.length == 5){
    			Channel = key_list[1];
            	OS = key_list[2];
            	Ver = key_list[3];
            	ProductType = key_list[4];
    		}
    		while(values.hasNext()){
    			String value = values.next().toString();
    			set_QRCode.add(value);                            //QR码扫描设备
        		ScanTypePV ++;                                    //QR码扫描次数
    		}
    		ScanTypeUV = set_QRCode.size();
    		FUN_ScanResultToDB("DA_Scan_Analysis",ScanType,ScanTypeUV,ScanTypePV, Date, OS, Ver, Channel,ProductType, output);
//    		output.collect(new Text(key.toString()), new Text(ScanTypeUV+"\t"+ScanTypePV));
    	}else if(key_list[0].equals("scan2DCode_DMCodeFlag")){
    		HashSet<String> set_QRCode = new HashSet<String>();
    		ScanType = "DMCode";
    		if(key_list.length == 5){
    			Channel = key_list[1];
            	OS = key_list[2];
            	Ver = key_list[3];
            	ProductType = key_list[4];
    		}
    		while(values.hasNext()){
    			String value = values.next().toString();
    			set_QRCode.add(value);                            //DM码扫描设备
        		ScanTypePV ++;                                    //DM码扫描次数
    		}
    		ScanTypeUV = set_QRCode.size();
    		FUN_ScanResultToDB("DA_Scan_Analysis",ScanType,ScanTypeUV,ScanTypePV, Date, OS, Ver, Channel,ProductType, output);
//    		output.collect(new Text(key.toString()), new Text(ScanTypeUV+"\t"+ScanTypePV));
    	}/*else if(key_list[0].equals("scan2DCode_HCodeFlag")){
    		HashSet<String> set_QRCode = new HashSet<String>();
    		ScanType = "HCode";
    		if(key_list.length == 5){
    			Channel = key_list[1];
            	OS = key_list[2];
            	Ver = key_list[3];
            	ProductType = key_list[4];
    		}
    		while(values.hasNext()){
    			String value = values.next().toString();
    			set_QRCode.add(value);                            //H码扫描设备
        		ScanTypePV ++;                                    //H码扫描次数
    		}
    		ScanTypeUV = set_QRCode.size();
    		FUN_ScanResultToDB("DA_Scan_Analysis",ScanType,ScanTypeUV,ScanTypePV, Date, OS, Ver, Channel,ProductType, output);
//    		output.collect(new Text(key.toString()), new Text(ScanTypeUV+"\t"+ScanTypePV));
    	}*/else if(key_list[0].equals("scan2DCode_WEPCCodeFlag")){
    		HashSet<String> set_QRCode = new HashSet<String>();
    		ScanType = "WEPCCode";
    		if(key_list.length == 5){
    			Channel = key_list[1];
            	OS = key_list[2];
            	Ver = key_list[3];
            	ProductType = key_list[4];
    		}
    		while(values.hasNext()){
    			String value = values.next().toString();
    			set_QRCode.add(value);                            //DM码扫描设备
        		ScanTypePV ++;                                    //DM码扫描次数
    		}
    		ScanTypeUV = set_QRCode.size();
    		FUN_ScanResultToDB("DA_Scan_Analysis",ScanType,ScanTypeUV,ScanTypePV, Date, OS, Ver, Channel,ProductType, output);
//    		output.collect(new Text(key.toString()), new Text(ScanTypeUV+"\t"+ScanTypePV));
    	}else if(key_list[0].equals("scan2DCode_HxCodeFlag")){
    		HashSet<String> set_QRCode = new HashSet<String>();
    		ScanType = "HCode";
    		if(key_list.length == 5){
    			Channel = key_list[1];
            	OS = key_list[2];
            	Ver = key_list[3];
            	ProductType = key_list[4];
    		}
    		while(values.hasNext()){
    			String value = values.next().toString();
    			set_QRCode.add(value);                            //汉信码扫描设备
        		ScanTypePV ++;                                    //汉信码扫描次数
    		}
    		ScanTypeUV = set_QRCode.size();
    		FUN_ScanResultToDB("DA_Scan_Analysis",ScanType,ScanTypeUV,ScanTypePV, Date, OS, Ver, Channel,ProductType, output);
//    		output.collect(new Text(key.toString()), new Text(ScanTypeUV+"\t"+ScanTypePV));
    	}else if(key_list[0].equals("scanExpressFlag")){
    		HashSet<String> Express_set = new HashSet<String>();
    		ScanType = "Express";
    		if(key_list.length == 5){
    			Channel = key_list[1];
            	OS = key_list[2];
            	Ver = key_list[3];
            	ProductType = key_list[4];
    		}
    		while(values.hasNext()){
    			String value = values.next().toString();
    			Express_set.add(value);                           //快递单扫描设备
        		ScanTypePV ++;                                    //快递单扫描次数
    		}
    		ScanTypeUV = Express_set.size();
    		FUN_ScanResultToDB("DA_Scan_Analysis",ScanType,ScanTypeUV,ScanTypePV, Date, OS, Ver, Channel,ProductType, output);
//    		output.collect(new Text(key.toString()), new Text(ScanTypeUV+"\t"+ScanTypePV));
    	}else if(key_list[0].equals("scanDrugFlag")){
    		HashSet<String> Drug_set = new HashSet<String>();
    		ScanType = "Drug";
    		if(key_list.length == 5){
    			Channel = key_list[1];
            	OS = key_list[2];
            	Ver = key_list[3];
            	ProductType = key_list[4];
    		}
    		while(values.hasNext()){
    			String value = values.next().toString();
    			Drug_set.add(value);                              //药监码扫描用户数
        		ScanTypePV ++;                                    //药监码扫描次数
    		}
    		ScanTypeUV = Drug_set.size();
    		FUN_ScanResultToDB("DA_Scan_Analysis",ScanType,ScanTypeUV,ScanTypePV, Date, OS, Ver, Channel,ProductType, output);
    	}
    }
    public void FUN_ScanResultToDB(String tableName,String ScanType,long ScanTypeUV,long ScanTypePV,String Date,String OS,String Ver,String Channel,String ProductType,OutputCollector<Text, Text> output){
		String[] volConName = { "Date", "OS", "Ver", "Channel","ProductType"};
		String[] volConValue = { Date, OS, Ver, Channel,ProductType};
		String[] volNameInsert = { "Date", "OS", "Ver", "Channel","ProductType" ,ScanType+"UV", ScanType+"PV" };
		String[] volValueInsert = { Date, OS, Ver, Channel,ProductType, ScanTypeUV+"", ScanTypePV+"" };
		String[] VName = { ScanType+"UV", ScanType+"PV" };
		String[] VValue = { ScanTypeUV+"", ScanTypePV+"" };	
		//			JobManager.ResultToDB(conn_230, tableName, volConName, volConValue, volNameInsert, volValueInsert, VName, VValue);
		try {
			JobManager.ResultToDB(output, tableName, volConName, volConValue, volNameInsert, volValueInsert, VName, VValue,"");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
		job_date =job.get("job_date");
	}
	
}
