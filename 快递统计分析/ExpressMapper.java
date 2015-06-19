package com.wochacha.da.transform.express;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.wochacha.da.model.JobManager;
import com.wochacha.da.model.Record;
import com.wochacha.da.util.HBaseCategory;
import com.wochacha.da.util.HBaseDriverManager;

public class ExpressMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	public Connection conn_pri = null;
	public static HashMap<String,String> expName_hm = null;
	public void map(LongWritable key, Text val, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {	
		String[] arr = val.toString().split("\t");	
        String UDID = arr[2];
        String Channel = arr[3];
		String OS = arr[4];
		String Ver = arr[5];
		String CTID = arr[6];
		String Action = arr[7];
		String DirectObject = arr[8];
		String IndirectObject = arr[9];
		String Result = arr[10];
		String Remarks = arr[11];
		String Message = arr[12];
		String recordFlag = arr[13];
		String CTID_whole = "4275";	
		OS = OS.trim();
		Action = Action.trim();
		DirectObject = DirectObject.trim();
		
		if (conn_pri == null) {
			conn_pri = JobManager.getUserReportDao();
			if (conn_pri == null) {
				output.collect(
						new Text("Error_log\tConnect Database da error!"),
						new Text(key.toString()));
				return;
			}
		}
		if (expName_hm == null) {
			getExpNameHM();
		}
		try {				                                                                                  
			if(!(Action.equalsIgnoreCase("Access") && DirectObject.equalsIgnoreCase("Express"))&&
			   !(Action.equalsIgnoreCase("add.express") && DirectObject.equalsIgnoreCase("Express"))&&
			   !(Action.equalsIgnoreCase("cancelFav.express") && DirectObject.equalsIgnoreCase("Express"))&&
			   !(Action.equalsIgnoreCase("delete") && DirectObject.equalsIgnoreCase("Express"))){        //快递各项分析 -- 快递总体情况
				output.collect(new Text("ExpressTotal"+"\t"+OS+"\t"+Ver+"\t"+CTID), new Text(UDID));
				output.collect(new Text("ExpressTotal"+"\t"+OS+"\t"+Ver+"\t"+CTID_whole), new Text(UDID));
			}			
			
			if(Action.equalsIgnoreCase("click.search") && DirectObject.equalsIgnoreCase("Express")){     //快递各项分析 -- 点击查询（快递查询页）
				output.collect(new Text("clickSearch"+"\t"+OS+"\t"+Ver+"\t"+CTID), new Text(UDID));
				output.collect(new Text("clickSearch"+"\t"+OS+"\t"+Ver+"\t"+CTID_whole), new Text(UDID));
			}else if(Action.equalsIgnoreCase("click.express") && DirectObject.equalsIgnoreCase("Express")){//快递各项分析 -- 选择快递公司(快递查询页)
				output.collect(new Text("clickExp_search"+"\t"+OS+"\t"+Ver+"\t"+CTID), new Text(UDID));
				output.collect(new Text("clickExp_search"+"\t"+OS+"\t"+Ver+"\t"+CTID_whole), new Text(UDID));
				
				String expressName = IndirectObject; 
				expressName = getExpName(Ver, expressName);
				output.collect(new Text("chooseExpress"+"\t"+OS+"\t"+Ver+"\t"+CTID+"\t"+expressName), new Text(UDID));
				output.collect(new Text("chooseExpress"+"\t"+OS+"\t"+Ver+"\t"+CTID_whole+"\t"+expressName), new Text(UDID));
			}else if(Action.equalsIgnoreCase("Access.search") && DirectObject.equalsIgnoreCase("Express")){//快递各项分析 -- 切换tab（快递查询）
				output.collect(new Text("switchSearch"+"\t"+OS+"\t"+Ver+"\t"+CTID), new Text(UDID));
				output.collect(new Text("switchSearch"+"\t"+OS+"\t"+Ver+"\t"+CTID_whole), new Text(UDID));
			}else if(Action.equalsIgnoreCase("Access.call") && DirectObject.equalsIgnoreCase("Express")){//快递各项分析 -- 切换tab（叫快递）
				output.collect(new Text("switchCall"+"\t"+OS+"\t"+Ver+"\t"+CTID), new Text(UDID));
				output.collect(new Text("switchCall"+"\t"+OS+"\t"+Ver+"\t"+CTID_whole), new Text(UDID));
			}else if(Action.equalsIgnoreCase("Access.charge") && DirectObject.equalsIgnoreCase("Express")){//快递各项分析 -- 切换tab（查运费）
				output.collect(new Text("switchCharge"+"\t"+OS+"\t"+Ver+"\t"+CTID), new Text(UDID));
				output.collect(new Text("switchCharge"+"\t"+OS+"\t"+Ver+"\t"+CTID_whole), new Text(UDID));
			}else if(Action.equalsIgnoreCase("Access.exhistory") && DirectObject.equalsIgnoreCase("Express")){//快递各项分析 -- 切换tab（查历史）
				output.collect(new Text("switchHistory"+"\t"+OS+"\t"+Ver+"\t"+CTID), new Text(UDID));
				output.collect(new Text("switchHistory"+"\t"+OS+"\t"+Ver+"\t"+CTID_whole), new Text(UDID));
			}else if(Action.equalsIgnoreCase("click.call") && DirectObject.equalsIgnoreCase("Express")){//快递各项分析 -- 叫快递（叫快递页面+查运费比价查询页）
				output.collect(new Text("clickCall"+"\t"+OS+"\t"+Ver+"\t"+CTID), new Text(UDID));
				output.collect(new Text("clickCall"+"\t"+OS+"\t"+Ver+"\t"+CTID_whole), new Text(UDID));
				
				String expressName = IndirectObject; 
				expressName = getExpName(Ver, expressName);
				output.collect(new Text("callExpress"+"\t"+OS+"\t"+Ver+"\t"+CTID+"\t"+expressName), new Text(UDID));
				output.collect(new Text("callExpress"+"\t"+OS+"\t"+Ver+"\t"+CTID_whole+"\t"+expressName), new Text(UDID));
				
			}else if(Action.equalsIgnoreCase("share.express") && DirectObject.equalsIgnoreCase("Express")){//快递各项分析 -- 分享快递查询结果
				output.collect(new Text("shareExpress"+"\t"+OS+"\t"+Ver+"\t"+CTID), new Text(UDID));
				output.collect(new Text("shareExpress"+"\t"+OS+"\t"+Ver+"\t"+CTID_whole), new Text(UDID));
			}else if(Action.equalsIgnoreCase("click.sentmessage") && DirectObject.equalsIgnoreCase("Express")){//快递各项分析 -- 点击通知发件（快递查询页+快递历史页）
				output.collect(new Text("clickSentMessage"+"\t"+OS+"\t"+Ver+"\t"+CTID), new Text(UDID));
				output.collect(new Text("clickSentMessage"+"\t"+OS+"\t"+Ver+"\t"+CTID_whole), new Text(UDID));
			}else if(Action.equalsIgnoreCase("sent.message") && DirectObject.equalsIgnoreCase("Express")){//快递各项分析 -- 短信发送（重发短信页）
				output.collect(new Text("sentMessage"+"\t"+OS+"\t"+Ver+"\t"+CTID), new Text(UDID));
				output.collect(new Text("sentMessage"+"\t"+OS+"\t"+Ver+"\t"+CTID_whole), new Text(UDID));
			}else if(Action.equalsIgnoreCase("compare.Price") && DirectObject.equalsIgnoreCase("Express")){//快递各项分析 -- 比价查询（查运费页）
				output.collect(new Text("comparePrice"+"\t"+OS+"\t"+Ver+"\t"+CTID), new Text(UDID));
				output.collect(new Text("comparePrice"+"\t"+OS+"\t"+Ver+"\t"+CTID_whole), new Text(UDID));
				
				if(Result.equals("0")){ //有运费结果
					output.collect(new Text("comparePrice_y"+"\t"+OS+"\t"+Ver+"\t"+CTID), new Text(UDID));
					output.collect(new Text("comparePrice_y"+"\t"+OS+"\t"+Ver+"\t"+CTID_whole), new Text(UDID));
				}else if(Result.equals("1")){//无运费结果
					output.collect(new Text("comparePrice_n"+"\t"+OS+"\t"+Ver+"\t"+CTID), new Text(UDID));
					output.collect(new Text("comparePrice_n"+"\t"+OS+"\t"+Ver+"\t"+CTID_whole), new Text(UDID));
				}
				
			}else if(Action.equalsIgnoreCase("search.express") && DirectObject.equalsIgnoreCase("barcode")){//快递各项分析 -- 进入快递结果
				String[] arrL = new String[]{"n","y","a"};
				String[][] arrR = new String[][] {
						{ "0", "a", "b", "f", "j", "w", "r" },
						{ "1", "c", "d", "e", "g", "h", "i", "k", "l", "m",
								"o", "p", "q", "x", "t", "u" } ,
								{"a"}
						};
				String tmpR="e";
				boolean flag = false;  
				for(int i=0;i<arrR.length&&!flag;i++){
					for(int j=0;j<arrR[i].length;j++){
						if(Result.equalsIgnoreCase(arrR[i][j])){
							tmpR=arrL[i];
							flag = true;
							break ;
						}
					}
				}	
				
				output.collect(new Text("searchExpress"+"\t"+OS+"\t"+Ver+"\t"+CTID), new Text(UDID));
				output.collect(new Text("searchExpress"+"\t"+OS+"\t"+Ver+"\t"+CTID_whole), new Text(UDID));
				
				output.collect(new Text("searchExpress_"+tmpR+"\t"+OS+"\t"+Ver+"\t"+CTID), new Text(UDID));
				output.collect(new Text("searchExpress_"+tmpR+"\t"+OS+"\t"+Ver+"\t"+CTID_whole), new Text(UDID));
				
				String expressName = Message; 
				expressName=getExpName(Ver,expressName);
				output.collect(new Text("scanExpress"+"\t"+OS+"\t"+Ver+"\t"+CTID+"\t"+expressName), new Text(UDID+"\t"+tmpR));
				output.collect(new Text("scanExpress"+"\t"+OS+"\t"+Ver+"\t"+CTID_whole+"\t"+expressName), new Text(UDID+"\t"+tmpR));
			}else if(Action.equalsIgnoreCase("show") && DirectObject.equalsIgnoreCase("Express")){//快递各项分析 -- 点击快递（快递大全页）
				output.collect(new Text("clickExp_show"+"\t"+OS+"\t"+Ver+"\t"+CTID), new Text(UDID));
				output.collect(new Text("clickExp_show"+"\t"+OS+"\t"+Ver+"\t"+CTID_whole), new Text(UDID));
				
				String expressName = IndirectObject; 
				expressName = getExpName(Ver, expressName);
				output.collect(new Text("chooseExpress"+"\t"+OS+"\t"+Ver+"\t"+CTID+"\t"+expressName), new Text(UDID));
				output.collect(new Text("chooseExpress"+"\t"+OS+"\t"+Ver+"\t"+CTID_whole+"\t"+expressName), new Text(UDID));
			} else if (Action.equalsIgnoreCase("mark")
					&& DirectObject.equalsIgnoreCase("Express")) {
				if (Result.equalsIgnoreCase("1")) {
					output.collect(new Text("markExpress_yes"+"\t"+OS+"\t"+Ver+"\t"+CTID), new Text(UDID));
					output.collect(new Text("markExpress_yes"+"\t"+OS+"\t"+Ver+"\t"+CTID_whole), new Text(UDID));
					
					String expressName = Message; 
					expressName = getExpName(Ver, expressName);
					output.collect(new Text("markExpress"+"\t"+OS+"\t"+Ver+"\t"+CTID+"\t"+expressName), new Text(UDID));
					output.collect(new Text("markExpress"+"\t"+OS+"\t"+Ver+"\t"+CTID_whole+"\t"+expressName), new Text(UDID));
				} else if (Result.equalsIgnoreCase("2")) {
					output.collect(new Text("markExpress_cancel"+"\t"+OS+"\t"+Ver+"\t"+CTID), new Text(UDID));
					output.collect(new Text("markExpress_cancel"+"\t"+OS+"\t"+Ver+"\t"+CTID_whole), new Text(UDID));
				}
			}else if(Action.equalsIgnoreCase("click.message")&&DirectObject.equalsIgnoreCase("message")&&IndirectObject.equalsIgnoreCase("3")){
				output.collect(new Text("clickmessage"+"\t"+OS+"\t"+Ver+"\t"+CTID), new Text(UDID));
				output.collect(new Text("clickmessage"+"\t"+OS+"\t"+Ver+"\t"+CTID_whole), new Text(UDID));
			}else if(Action.equalsIgnoreCase("push.message")&&DirectObject.equalsIgnoreCase("express")){
				output.collect(new Text("pushmessage"+"\t"+OS+"\t"+Ver+"\t"+CTID), new Text(UDID));
				output.collect(new Text("pushmessage"+"\t"+OS+"\t"+Ver+"\t"+CTID_whole), new Text(UDID));
			}
		} catch (Exception e) {
			output.collect(new Text("err_log"), new Text(val.toString()));
			// e.printStackTrace();
		}
	}
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		if(conn_pri != null){
			try{
				conn_pri.close();
				conn_pri = null;
			}catch(Exception e){
				
			}
			conn_pri = null;			
		}
		super.close();
	}
	public void getExpNameHM(){
		if(expName_hm==null){				
			String sql = "SELECT SQL_NO_CACHE Com,Name FROM WCC_Express_Com where state=1";
			expName_hm = new HashMap<String,String>();
			PreparedStatement pstmt;
			try {
				pstmt = conn_pri.prepareStatement(sql);
				ResultSet rs = pstmt.executeQuery();
				String Com = "";
				String Name = "";
				while (rs.next()) {
					Com = rs.getString("Com");
					Name = rs.getString("Name");
					if (Com != null) {
						Com = Com.replace(" ", "");
					}
					if (Name != null) {
						Name = Name.replace(" ", "");
					}					
					expName_hm.put(Com, Name);
				}
				rs.close();
				pstmt.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block				
				return ;
			}
		}		
	}
	public String getExpName(String Ver,String expressName){
		String Name = expressName;
		boolean flg=false;
		if(Ver!=null && Ver.length()!=0 && Ver.indexOf(".")>0){
			int tmpV0 = 0;
			int tmpV1 = 0;
			String[] tmp=Ver.split("\\.");
			tmpV0=Integer.parseInt(tmp[0]);
			if(tmp.length>=2){
				tmpV1=Integer.parseInt(tmp[1]);
			}
			if (tmpV0 > 6 || (tmpV0 == 6 && tmpV1 >= 5)) {
				if (expName_hm.containsKey(expressName)) {
					Name = expName_hm.get(expressName);
					flg=true;
				}
			}
		}
		if(!flg){
			if (expName_hm.containsKey(expressName)) {
				Name = expName_hm.get(expressName);
			}
		}
		return Name;
	}
	
}
