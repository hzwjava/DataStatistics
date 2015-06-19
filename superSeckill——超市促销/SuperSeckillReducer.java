package com.wochacha.da.transform.superSeckill;

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
public class SuperSeckillReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {   
//	public static Connection conn_da = null;
	public String job_date = "";
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
//    	 if(conn_da == null){
//    		 conn_da =  JobManager.getResultDao();
//    		 if(conn_da == null){
//    			 throw new IOException();  			 
//    		 }
//		 }      	
     	String Date = job_date;     	
     	String[] key_list = key.toString().split("\t");          	
     	if(key_list[0].equalsIgnoreCase("LowSecKill")){		     		
     		if (key_list.length == 3) {     								
    			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("BYShopping")){		     		
     		if (key_list.length == 3) {     						
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("Poster")){		     		
     		if (key_list.length == 3) {     						
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("Search")){		     		
     		if (key_list.length == 3) {     								
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("LowSecKillPromotion")){		     		
     		if (key_list.length == 3) {     								
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("BYShoppingPromotion")){		     		
     		if (key_list.length == 3) {     								
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("LowSecKillSupMSwitch")){		     		
     		if (key_list.length == 3) {     								
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("BYShoppingSearchCom")){		     		
     		if (key_list.length == 3) {     								
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("SearchSupM")){		     		
     		if (key_list.length == 3) {     								
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("InCartQuiscan")){		     		
     		if (key_list.length == 3) {     								
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("InCartCompPri")){		     		
     		if (key_list.length == 3) {     								
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("InCartComDetail")){		     		
     		if (key_list.length == 3) {     								
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("InCartOrderMan")){		     		
     		if (key_list.length == 3) {     								
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].startsWith("ClassifyPage_")){		     		
     		if (key_list.length == 3) {     			
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("KeySearchReSearchCom")){		     		
     		if (key_list.length == 3) {     								
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("KeySearchReSearchSupM")){		     		
     		if (key_list.length == 3) {     								
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("LowSecKillSelectEstate")){		     		
     		if (key_list.length == 3) {     								
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("LowSecKillSKP")){		     		
     		if (key_list.length == 3) {     								
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("LowSecKillSubOrder")){		     		
     		if (key_list.length == 3) {     								
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("LowSecKillPayOnline")){		     		
     		if (key_list.length == 3) {     								
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("BYShoppingSelectStore")){		     		
     		if (key_list.length == 3) {     								
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("BYShoppingCompPriCheck")){		     		
     		if (key_list.length == 3) {     								
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("BYShoppingCheckOut")){		     		
     		if (key_list.length == 3) {     								
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("BYShoppingSubOrder")){		     		
     		if (key_list.length == 3) {     								
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("LowSecKill_SkimPromotion")){		     		
     		if (key_list.length == 4) {     								
     			FUN_ComIdResultToDB("DA_SuperTop_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("BYShopping_SkimPromotion")){		     		
     		if (key_list.length == 4) {     								
     			FUN_ComIdResultToDB("DA_SuperTop_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("Poster_SkimPoster")){		     		
     		if (key_list.length == 4) {     								
     			FUN_ComIdResultToDB("DA_SuperTop_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("PosterSkimNum")){		     		
     		if (key_list.length == 3) {     								
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("SearchSuperM")){		     		
     		if (key_list.length == 4) {     								
     			FUN_ComIdResultToDB("DA_SuperTop_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("BYShopping_SelectStore")){		     		
     		if (key_list.length == 4) {     								
     			FUN_ComIdResultToDB("DA_SuperTop_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("SupShopsale")){		     		
     		if (key_list.length == 3) {     								
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("SupList")){		     		
     		if (key_list.length == 3) {     								
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("SupBill")){		     		
     		if (key_list.length == 3) {     								
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("Shopping_Poster")){
     		if (key_list.length == 4) {     								
     			FUN_ComIdResultToDB("DA_SuperTop_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("LowSecKillPromotionDetail")){		     		
     		if (key_list.length == 3) {     								
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("BYShoppingPromotionDetail")){		     		
     		if (key_list.length == 3) {     								
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("SearchSupM_poster")){		     		
     		if (key_list.length == 3) {     								
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("SearchSupM_find")){		     		
     		if (key_list.length == 3) {     								
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("ShoppingActive")){		     		
     		if (key_list.length == 3) {     								
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("Shopping_active")){
     		if (key_list.length == 4) {     								
     			FUN_ComIdResultToDB("DA_SuperTop_Analysis",values,key.toString(),Date, output);
    		}
     	}else if (key_list[0].equalsIgnoreCase("ProFavIndex")) {
			if (key_list.length == 3) {
				FUN_CPUResultToDB("DA_SuperDetail_Analysis", values,
						key.toString(), Date, output);
			}
		} else if (key_list[0].equalsIgnoreCase("ProFavList")) {
			if (key_list.length == 3) {
				FUN_CPUResultToDB("DA_SuperDetail_Analysis", values,
						key.toString(), Date, output);
			}
		} else if (key_list[0].equalsIgnoreCase("ProNotFavIndex")) {
			if (key_list.length == 3) {
				FUN_CPUResultToDB("DA_SuperDetail_Analysis", values,
						key.toString(), Date, output);
			}
		} else if (key_list[0].equalsIgnoreCase("ProNotFavList")) {
			if (key_list.length == 3) {
				FUN_CPUResultToDB("DA_SuperDetail_Analysis", values,
						key.toString(), Date, output);
			}
		} else if (key_list[0].equalsIgnoreCase("PriceCompare")) {
			if (key_list.length == 3) {
				FUN_CPUResultToDB("DA_SuperDetail_Analysis", values,
						key.toString(), Date, output);
			}
		} else if (key_list[0].equalsIgnoreCase("StoreList")) {
			if (key_list.length == 3) {
				FUN_CPUResultToDB("DA_SuperDetail_Analysis", values,
						key.toString(), Date, output);
			}
		} else if (key_list[0].equalsIgnoreCase("SearchBox")) {
			if (key_list.length == 3) {
				FUN_CPUResultToDB("DA_SuperDetail_Analysis", values,
						key.toString(), Date, output);
			}
		}else if(key_list[0].equalsIgnoreCase("MinPriClickSup")){		     		
     		if (key_list.length == 3) {     								
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("MinPri_Sup")){
     		if (key_list.length == 4) {     								
     			FUN_ComIdResultToDB("DA_SuperTop_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("MinPriComparePri")){		     		
     		if (key_list.length == 3) {     								
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("MinPri_SkimCom")){
     		if (key_list.length == 4) {     								
     			FUN_ComIdResultToDB("DA_SuperTop_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].startsWith("MinPriTab_")){		     		
     		if (key_list.length == 3) {     			
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("FruitShopping")){		     		
     		if (key_list.length == 3) {     			
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("FruitShop_wap")){		     		
     		if (key_list.length == 3) {     			
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("FShoppingSwitchTab")){		     		
     		if (key_list.length == 3) {     			
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("FShopping_Topic")){		     		
     		if (key_list.length == 4) {     			
     			FUN_ComIdResultToDB("DA_SuperTop_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("FShoppingComSkim")){		     		
     		if (key_list.length == 3) {     			
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("FshopComSkim_wap")){		     		
     		if (key_list.length == 3) {     			
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("FShopping_SkimCom")){		     		
     		if (key_list.length == 4) {     			
     			FUN_ComIdResultToDB("DA_SuperTop_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("FShoppingBuyNow")){		     		
     		if (key_list.length == 3) {     			
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("FShopBuyNow_wap")){		     		
     		if (key_list.length == 3) {     			
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("FShoppingBasket")){		     		
     		if (key_list.length == 3) {     			
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("FShopBasket_wap")){		     		
     		if (key_list.length == 3) {     			
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("FShoppingOrderConfirm")){		     		
     		if (key_list.length == 3) {     			
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("FShopOrderConfirm_wap")){		     		
     		if (key_list.length == 3) {     			
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("FShoppingTotal")){		     		
     		if (key_list.length == 3) {     			
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("SupShoppingTotal")){		     		
     		if (key_list.length == 3) {     			
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("ShakePromotion")){		     		
     		if (key_list.length == 3) {     			
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("ShakeFShopping")){		     		
     		if (key_list.length == 3) {     			
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("TShopping")){		     		
     		if (key_list.length == 3) {     			
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("TShoppingSwitchTab")){		     		
     		if (key_list.length == 3) {     			
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("TShopping_Topic")){		     		
     		if (key_list.length == 4) {     			
     			FUN_ComIdResultToDB("DA_SuperTop_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("TShoppingComSkim")){		     		
     		if (key_list.length == 3) {     			
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("TShopping_SkimCom")){		     		
     		if (key_list.length == 4) {     			
     			FUN_ComIdResultToDB("DA_SuperTop_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("TShoppingBuyNow")){		     		
     		if (key_list.length == 3) {     			
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("TShoppingBasket")){		     		
     		if (key_list.length == 3) {     			
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("TShoppingOrderConfirm")){		     		
     		if (key_list.length == 3) {     			
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("TShoppingTotal")){		     		
     		if (key_list.length == 3) {     			
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("SupmarketAll")){		     		
     		if (key_list.length == 3) {     			
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("SwitchLife")){		     		
     		if (key_list.length == 3) {     			
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("ClickLife")){		     		
     		if (key_list.length == 3) {     			
     			FUN_CPUResultToDB("DA_SuperDetail_Analysis",values,key.toString(),Date, output);
    		}
     	}else if(key_list[0].equalsIgnoreCase("ClickLife_website")){		     		
     		if (key_list.length == 4) {     			
     			FUN_ComIdResultToDB("DA_SuperTop_Analysis",values,key.toString(),Date, output);
    		}
     	}
    }
    public void FUN_CPUResultToDB(String tableName,Iterator<Text> values,String key,String Date,OutputCollector<Text, Text> output){
    	int CPV = 0; //访问量	 
    	int UV = 0;	 //访问用户
    	String UDID = "";
    	String OS = "";
     	String CTID = "";
		String FunType = "";   
    	HashSet<String> udid_set = new HashSet<String>();
    	String[] key_list = key.split("\t");       	
    	FunType = key_list[0];
		OS = key_list[1];
		CTID = key_list[2];    			
    	while (values.hasNext()) {
			String[] value = values.next().toString().split("\t");
			UDID = value[0];
			udid_set.add(UDID);
			CPV++;
		}
		UV = udid_set.size();  
		
		String[] volConName = { "Date","OS","CTID","FunType"};
		String[] volConValue = { Date,OS,CTID,FunType};
		String[] volNameInsert = { "Date","OS","CTID","FunType","CPV","UV"};
		String[] volValueInsert = { Date,OS,CTID,FunType,CPV+"",UV+""};
		String[] VName = { "CPV","UV"};
		String[] VValue = { CPV+"",UV+""};	
		//			JobManager.ResultToDB(conn_da, tableName, volConName, volConValue, volNameInsert, volValueInsert, VName, VValue);
		try {
			JobManager.ResultToDB(output, tableName, volConName, volConValue, volNameInsert, volValueInsert, VName, VValue,"");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			MsgSender.sendMessage(new String[] {"err","Insert error\t"+job_date+"\tSuperSecKillReducer"+FunType+"\t"+e.getMessage()});
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			MsgSender.sendMessage(new String[] {"err","Insert error\t"+job_date+"\tSuperSecKillReducer"+FunType+"\t"+e.getMessage()});
		}
	}
    public void FUN_ComIdResultToDB(String tableName,Iterator<Text> values,String key,String Date,OutputCollector<Text, Text> output){
    	int PV = 0; //访问量	 
    	int UV = 0;	 //访问用户
    	String UDID = "";
    	String OS = "";
     	String CTID = "";
		byte FunType = -1;
		String ObjectId = "";  
		String Result="";
    	HashSet<String> udid_set = new HashSet<String>();
    	HashSet<String> ppid_set = new HashSet<String>();
    	String[] key_list = key.split("\t");     	
    	if(key_list[0].equalsIgnoreCase("LowSecKill_SkimPromotion")){
    		FunType = 1;
    	}else if(key_list[0].equalsIgnoreCase("BYShopping_SkimPromotion")){
    		FunType = 3;
    	}else if(key_list[0].equalsIgnoreCase("Poster_SkimPoster")){
    		FunType = 5;
    	}else if(key_list[0].equalsIgnoreCase("SearchSuperM")){
    		FunType = 6;
    	}else if(key_list[0].equalsIgnoreCase("BYShopping_SelectStore")){
    		FunType = 7;
    	}else if(key_list[0].equalsIgnoreCase("MinPri_Sup")){
    		FunType = 8;
    	}else if(key_list[0].equalsIgnoreCase("MinPri_SkimCom")){
    		FunType = 9;
    	}else if(key_list[0].equalsIgnoreCase("Shopping_active")){
    		FunType = 10; 
    	}else if(key_list[0].equalsIgnoreCase("Fshopping_BuyCom")){
    		FunType = 11;
    	}else if(key_list[0].equalsIgnoreCase("Fshopping_Topic")){
    		FunType = 12;
    	}else if(key_list[0].equalsIgnoreCase("Shopping_Poster")){
    		FunType = 13;
    	}else if(key_list[0].equalsIgnoreCase("FShopping_SkimCom")){
    		FunType = 14;
    	}else if(key_list[0].equalsIgnoreCase("TShopping_SkimCom")){
    		FunType = 15;
    	}else if(key_list[0].equalsIgnoreCase("Tshopping_Topic")){
    		FunType = 16;
    	}else if(key_list[0].equalsIgnoreCase("ClickLife_website")){
    		FunType = 17;
    	}
    	
    	while (values.hasNext()) {
			String[] value = values.next().toString().split("\t");
			UDID = value[0];
			udid_set.add(UDID);
			if (FunType == 13) {
				String PPID = "";
				if (value.length == 2) {
					PPID = value[1];
					ppid_set.add(PPID);
				}
			}
			PV++;
		}
		UV = udid_set.size();  
    	OS = key_list[1];
		CTID = key_list[2]; 
		if (FunType == 1 || FunType == 3 || FunType == 5 || FunType == 7
				|| FunType == 8 || FunType == 10 || FunType == 12
				|| FunType == 14 || FunType == 15 || FunType == 16) {
			ObjectId = key_list[3];
			String[] volConName = { "Date","OS","CTID","FunType","ObjectId"};
			String[] volConValue = { Date,OS,CTID,FunType+"",ObjectId};
			String[] volNameInsert = { "Date","OS","CTID","FunType","ObjectId","UV","PV"};
			String[] volValueInsert = { Date,OS,CTID,FunType+"",ObjectId,UV+"",PV+""};
			String[] VName = { "UV","PV"};
			String[] VValue = { UV+"",PV+""};	
			//				JobManager.ResultToDB(conn_da, tableName, volConName, volConValue, volNameInsert, volValueInsert, VName, VValue);
			try {
				JobManager.ResultToDB(output, tableName, volConName, volConValue, volNameInsert, volValueInsert, VName, VValue,"");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}else if(FunType == 9){
			String[] tmp=key_list[3].split("_");
			if(tmp.length==2){
				ObjectId = tmp[0];
				Result = tmp[1];
				String[] volConName = { "Date","OS","CTID","FunType","ObjectId","Result"};
				String[] volConValue = { Date,OS,CTID,FunType+"",ObjectId,Result};
				String[] volNameInsert = { "Date","OS","CTID","FunType","ObjectId","Result","UV","PV"};
				String[] volValueInsert = { Date,OS,CTID,FunType+"",ObjectId,Result,UV+"",PV+""};
				String[] VName = { "UV","PV"};
				String[] VValue = { UV+"",PV+""};	
				try {
//					JobManager.ResultToDB(conn_da, tableName, volConName, volConValue, volNameInsert, volValueInsert, VName, VValue);			
					JobManager.ResultToDB(output, tableName, volConName, volConValue, volNameInsert, volValueInsert, VName, VValue,"");
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					MsgSender.sendMessage(new String[] {"err","Insert error\t"+job_date+"\tSuperSecKillReducer"+FunType+"\t"+e.getMessage()});
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					MsgSender.sendMessage(new String[] {"err","Insert error\t"+job_date+"\tSuperSecKillReducer"+FunType+"\t"+e.getMessage()});
				}
			}
		}else if(FunType == 6){
			ObjectId = key_list[3].substring(0,key_list[3].length()-2);
			Result = key_list[3].substring(key_list[3].length()-1,key_list[3].length());
			String[] volConName = { "Date","OS","CTID","FunType","ObjectId","Result"};
			String[] volConValue = { Date,OS,CTID,FunType+"",ObjectId,Result};
			String[] volNameInsert = { "Date","OS","CTID","FunType","ObjectId","Result","UV","PV"};
			String[] volValueInsert = { Date,OS,CTID,FunType+"",ObjectId,Result,UV+"",PV+""};
			String[] VName = { "UV","PV"};
			String[] VValue = { UV+"",PV+""};	
			//				JobManager.ResultToDB(conn_da, tableName, volConName, volConValue, volNameInsert, volValueInsert, VName, VValue);
			try {
				JobManager.ResultToDB(output, tableName, volConName, volConValue, volNameInsert, volValueInsert, VName, VValue,"");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}else if(FunType == 13){
			ObjectId = key_list[3];
			Result = ppid_set.size()+"";
			String[] volConName = { "Date","OS","CTID","FunType","ObjectId","Result"};
			String[] volConValue = { Date,OS,CTID,FunType+"",ObjectId,Result};
			String[] volNameInsert = { "Date","OS","CTID","FunType","ObjectId","Result","UV","PV"};
			String[] volValueInsert = { Date,OS,CTID,FunType+"",ObjectId,Result,UV+"",PV+""};
			String[] VName = { "UV","PV"};
			String[] VValue = { UV+"",PV+""};	
			try {
//				JobManager.ResultToDB(conn_da, tableName, volConName, volConValue, volNameInsert, volValueInsert, VName, VValue);			
				JobManager.ResultToDB(output, tableName, volConName, volConValue, volNameInsert, volValueInsert, VName, VValue,"");
			}catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				MsgSender.sendMessage(new String[] {"err","Insert error\t"+job_date+"\tSuperSecKillReducer"+FunType+"\t"+e.getMessage()});
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				MsgSender.sendMessage(new String[] {"err","Insert error\t"+job_date+"\tSuperSecKillReducer"+FunType+"\t"+e.getMessage()});
			}
		}
	}
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
//		if(conn_da != null){
//			try{
//				conn_da.close();
//				conn_da = null;
//			}catch(Exception e){
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
