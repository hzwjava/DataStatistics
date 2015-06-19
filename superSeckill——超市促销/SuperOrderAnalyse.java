package com.wochacha.da.transform.superSeckill;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import com.wochacha.da.model.JobManager;
import com.wochacha.da.util.MsgSender;

public class SuperOrderAnalyse {
	public Connection conn_order = null;
	public Connection conn_da = null;
	public boolean OrderDetailAnalyse(String strDayN,String OrderType,String cityType){			
		 if(conn_order == null){
			 conn_order = JobManager.getPriDao();
	   		 if(conn_order == null){
	   			 System.out.println("connection order failed!");
	   			 return false;
	   		 }
		 } 	
		String Date = strDayN;
		String OS = "";
		String CTID = "";
		String sql="";
		int OrderUsrNum = 0;
		int OrderNum = 0;
		float OrderAmount = 0;
		OS=JobManager.OSdeal(OS);
		if(OrderType.equals("1")){ 		 //底价秒杀订单分析
			if(cityType.equals("wholeCity")){		//不分城市
				sql = "SELECT count(distinct URID) as OrderUsrNum, count(distinct ORID) as OrderNum,sum(Amount) as OrderAmount from ("+
					  "SELECT DISTINCT o.ORID,o.URID,o.Amount FROM WCC_MSOrder o INNER JOIN WCC_MSOrderlists l where "+
					  "(l.Status in ('4','7','16','17')) and o.CreatedTime>=? and o.CreatedTime<=? and o.type=2) t";
			}else if(cityType.equals("city")){  	//分城市
				sql = "SELECT t.CTID,count(distinct t.URID) as OrderUsrNum, count(distinct t.ORID) as OrderNum,sum(t.Amount) as OrderAmount FROM ("+
					  "SELECT distinct o.ORID, o.CTID,o.URID,o.Amount FROM WCC_MSOrder o INNER JOIN WCC_MSOrderlists l on o.ORID=l.ORID where "+
					  "(l.Status in ('4','7','16','17')) and o.CreatedTime>=? and o.CreatedTime<=? and o.type=2) t group by t.CTID";
			}			
		}else if(OrderType.equals("2")){ //超市代购订单分析
			if(cityType.equals("wholeCity")){		//不分城市
				sql = "SELECT count(distinct t.URID) as OrderUsrNum, count(distinct t.ORID) as OrderNum,sum(t.Amount) as OrderAmount FROM ("+
					  "SELECT Distinct B.ORID,B.URID,B.Amount FROM WCC_BYOrder B INNER JOIN WCC_BYOrderHistory h on B.ORID=h.ORID where "+
					  "(h.Status in ('4','7','16','17')) and B.CreatedTime>=? and B.CreatedTime<=?) t" ;
			}else if(cityType.equals("city")){	    //分城市
				sql = "SELECT CTID,count(distinct URID) as OrderUsrNum, count(distinct ORID) as OrderNum,sum(Amount) as OrderAmount from ("+
                      "SELECT Distinct B.ORID,B.URID,B.Amount,S.CTID FROM WCC_BYOrder B INNER JOIN GC_Store S INNER JOIN WCC_BYOrderHistory h "+
					  "on B.STID=S.STID and B.ORID=h.ORID where (h.Status in ('4','7','16','17')) and B.CreatedTime>= ? and "+
                      "B.CreatedTime<=?) t group by t.CTID";
			}			
		}else if(OrderType.equals("7")){//超市代购退款订单分析
			if(cityType.equals("wholeCity")){		//不分城市
				sql = "SELECT count(distinct t.URID) as OrderUsrNum, count(distinct t.ORID) as OrderNum,sum(t.Amount) as OrderAmount FROM ("+
					  "SELECT Distinct B.ORID,B.URID,B.Amount FROM WCC_BYOrder B left JOIN WCC_BYOrderOtherInfo o on B.ORID=o.ORID where "+
					  "o.cancelGoods!=0 and B.CreatedTime>=? and B.CreatedTime<=?) t";
			}else if(cityType.equals("city")){	    //分城市
				sql = "SELECT CTID,count(distinct t.URID) as OrderUsrNum, count(distinct t.ORID) as OrderNum,sum(t.Amount) as OrderAmount FROM("+
			          "SELECT Distinct B.ORID,S.CTID,B.URID,B.Amount FROM WCC_BYOrder B left JOIN WCC_BYOrderOtherInfo o on B.ORID=o.ORID "+
					  "left JOIN GC_Store S on B.STID=S.STID where o.cancelGoods!=0 and B.CreatedTime>=? and B.CreatedTime<=?) t group by t.CTID";
			}		
		}else if(OrderType.equals("9")){ //水果代购订单分析
			if(cityType.equals("wholeCity")){		//不分城市
				sql = "SELECT count(distinct UDID) as OrderUsrNum, count(distinct ID) as OrderNum,sum(Amount) as OrderAmount FROM "+
			          "WCC_Retail_Order where CreatedTime>=? and CreatedTime<=? and Tag=1 and Type=1 and State in (2,3,4,7)" ;
			}else if(cityType.equals("city")){	    //分城市
				sql = "SELECT CTID,count(distinct UDID) as OrderUsrNum, count(distinct ID) as OrderNum,sum(Amount) as OrderAmount FROM "+
			          "WCC_Retail_Order where CreatedTime>=? and CreatedTime<=? and Tag=1 and Type=1 and State in (2,3,4,7) group by CTID";
			}			
		}else if(OrderType.equals("10")){ //水果代购退款订单分析
			if(cityType.equals("wholeCity")){		//不分城市
				sql = "SELECT count(distinct UDID) as OrderUsrNum, count(distinct ID) as OrderNum,sum(Amount) as OrderAmount FROM "+
                      "WCC_Retail_Order where CreatedTime>=? and CreatedTime<=? and Tag=1 and Type=2" ;
			}else if(cityType.equals("city")){	    //分城市
				sql = "SELECT CTID,count(distinct UDID) as OrderUsrNum, count(distinct ID) as OrderNum,sum(Amount) as OrderAmount FROM "+
	                  "WCC_Retail_Order where CreatedTime>=? and CreatedTime<=? and Tag=1 and Type=2 group by CTID" ;
			}			
		}else if(OrderType.equals("11")){ //特产代购订单分析
			if(cityType.equals("wholeCity")){		//不分城市
				sql = "SELECT count(distinct UDID) as OrderUsrNum, count(distinct ID) as OrderNum,sum(Amount) as OrderAmount FROM "+
			          "WCC_Retail_Order where CreatedTime>=? and CreatedTime<=? and Tag=2 and Type=1 and State in (2,3,4,7)" ;
			}else if(cityType.equals("city")){	    //分城市
				sql = "SELECT CTID,count(distinct UDID) as OrderUsrNum, count(distinct ID) as OrderNum,sum(Amount) as OrderAmount FROM "+
			          "WCC_Retail_Order where CreatedTime>=? and CreatedTime<=? and Tag=2 and Type=1 and State in (2,3,4,7) group by CTID";
			}			
		}else if(OrderType.equals("12")){ //特产代购退款订单分析
			if(cityType.equals("wholeCity")){		//不分城市
				sql = "SELECT count(distinct UDID) as OrderUsrNum, count(distinct ID) as OrderNum,sum(Amount) as OrderAmount FROM "+
                      "WCC_Retail_Order where CreatedTime>=? and CreatedTime<=? and Tag=2 and Type=2 " ;
			}else if(cityType.equals("city")){	    //分城市
				sql = "SELECT CTID,count(distinct UDID) as OrderUsrNum, count(distinct ID) as OrderNum,sum(Amount) as OrderAmount FROM "+
	                  "WCC_Retail_Order where CreatedTime>=? and CreatedTime<=? and Tag=2 and Type=2 group by CTID" ;
			}			
		}
		PreparedStatement pstmt;
		ResultSet rs;
		try {
			pstmt = conn_order.prepareStatement(sql);
			String Daytime1 = strDayN + " 00:00:00";
			String Daytime2 = strDayN + " 23:59:59";
			pstmt.setString(1, Daytime1);
			pstmt.setString(2, Daytime2);
			rs = pstmt.executeQuery();	
			
			if(conn_da == null){
				conn_da =  JobManager.getResultDao();
		   		 if(conn_da == null){
		   			 System.out.println("connection da failed!");
		   			 return false;
		   		 }
			 } 
			
			while(rs.next()){	
				if(cityType.equals("wholeCity")){
					CTID = "4275";
				}else if(cityType.equals("city")){
					CTID = rs.getString("CTID");
					CTID=JobManager.CTIDdeal(CTID);
				}
				OrderUsrNum = rs.getInt("OrderUsrNum");
				OrderNum = rs.getInt("OrderNum");
				OrderAmount = rs.getFloat("OrderAmount");				
				String[] volName = new String[]{"Date","OS", "CTID","OrderType", "OrderUsrNum", "OrderNum","OrderAmount"};
				String[] volValue = new String[]{Date,OS, CTID,OrderType, OrderUsrNum+"", OrderNum+"",OrderAmount+""};
				String[] volConName = { "Date","OS", "CTID","OrderType" };
				String[] volConValue = { Date,OS, CTID ,OrderType};
				String[] Vname = new String[] { "OrderUsrNum", "OrderNum","OrderAmount" };
				String[] Vvalue = new String[] { OrderUsrNum+"", OrderNum+"",OrderAmount+"" };
				try {
					//订单明细分析
//					JobManager.ResultToDB(conn_da, "DA_OrderDetail_Analysis", volConName, volConValue, volName, volValue, Vname, Vvalue);
					JobManager.DBToFile( "DA_OrderDetail_Analysis", volConName, volConValue, volName, volValue, Vname, Vvalue,"");
	        	} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					MsgSender.sendMessage(new String[] {"err","Insert error\t"+Date+"\tSuperOrderDetailAnalyse\t"+e.getMessage()});
				}				
			}
			rs.close();
			pstmt.close();
			conn_order.close();
			conn_da.close();
			conn_order=null;
			conn_da=null;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			MsgSender.sendMessage(new String[] {"err","Select error\t"+sql+"\tSuperOrderDetailAnalyse\t"+e.getMessage()});
			if (conn_order != null || conn_da != null) {
				try {
					conn_order.close();
					conn_da.close();
				} catch (Exception ee) {

				}
				conn_order = null;
				conn_da = null;
			}			
			return false;
		}
		return true;
	}
	public boolean OrderBuyAnalyse(String strDayN,String OrderType,String cityType){		
		 if(conn_order == null){
			 conn_order = JobManager.getPriDao();
	   		 if(conn_order == null){
	   			 System.out.println("connection order failed!");
	   			 return false;
	   		 }
		 } 	
		String Date = strDayN;
		String OS = "";
		String CTID = "";
		String sql = "";
		String Result = "";
		String ObjectId = "";
		String FunType = "";
		int UV = 0;
		int PV = 0;	
		OS=JobManager.OSdeal(OS);
		if(OrderType.equals("1")){ 		 //底价秒杀商品购买
			if(cityType.equals("wholeCity")){	  	//不分城市
				sql = "select t.PPPID as ObjectId,t.Type,count(distinct t.URID) as UV,count(*) as PV from ("+
                      "select DISTINCT o.ORID,o.URID,o.Type,o.PPPID from WCC_MSOrder o INNER JOIN WCC_MSOrderlists l on o.ORID=l.ORID "+
                      "where (l.Status in ('4','7','16','17')) and o.CreatedTime>=? and o.CreatedTime<=? ) t group by t.PPPID,t.Type";
			}else if(cityType.equals("city")){ 		//分城市
				sql = "select t.CTID,t.PPPID as ObjectId,t.Type,count(distinct t.URID) as UV,count(*) as PV from ("+
					  "SELECT DISTINCT o.ORID,o.URID,o.Type,o.PPPID,o.CTID from WCC_MSOrder o INNER JOIN WCC_MSOrderlists l on o.ORID=l.ORID where "+
					  "(l.Status in ('4','7','16','17')) and o.CreatedTime>=? and o.CreatedTime<=? ) t group by t.PPPID,t.Type,t.CTID";
			}			
			FunType="2";
		}else if(OrderType.equals("2")){ //超市代购商品购买
			if(cityType.equals("wholeCity")){   	//不分城市
				sql = "SELECT t.BPRID as ObjectId,count(distinct t.URID) as UV,sum(t.Number) as PV FROM ("+
					  "SELECT DISTINCT B.ORID,B.URID,D.BPRID,D.Number FROM WCC_BYOrder B INNER JOIN WCC_BYOrderDetail D INNER JOIN WCC_BYOrderHistory h "+
					  "on B.ORID=D.ORID and B.ORID=h.ORID where (h.Status in ('4','7','16','17')) and B.CreatedTime>=? and B.CreatedTime<=?) t group by t.BPRID";
			} else if (cityType.equals("city")) {   // 分城市
				sql = "SELECT t.CTID,t.BPRID as ObjectId,count(distinct t.URID) as UV,sum(t.Number) as PV FROM ("+
					  "SELECT DISTINCT B.ORID,D.BPRID,B.URID,D.Number,S.CTID FROM WCC_BYOrder B INNER JOIN WCC_BYOrderDetail D INNER JOIN GC_Store S INNER JOIN WCC_BYOrderHistory h "+
					  "on B.STID=S.STID and B.ORID=D.ORID and B.ORID=h.ORID where (h.Status in ('4','7','16','17')) and B.CreatedTime>=? and B.CreatedTime<=?) t group by t.CTID,t.BPRID";
			}
			FunType="4";
		}
		PreparedStatement pstmt;
		ResultSet rs;
		try {
			pstmt = conn_order.prepareStatement(sql);
			String Daytime1 = strDayN + " 00:00:00";
			String Daytime2 = strDayN + " 23:59:59";
			pstmt.setString(1, Daytime1);
			pstmt.setString(2, Daytime2);
			rs = pstmt.executeQuery();	
			
			if(conn_da == null){
				conn_da =  JobManager.getResultDao();
		   		 if(conn_da == null){
		   			 System.out.println("connection da failed!");
		   			 return false;
		   		 }
			 } 
			
			while(rs.next()){	
				if(cityType.equals("wholeCity")){
					CTID = "4275";
				}else if(cityType.equals("city")){
					CTID = rs.getString("CTID");
					CTID = JobManager.CTIDdeal(CTID);
				}
				ObjectId = rs.getString("ObjectId");
				UV = rs.getInt("UV");
				PV = rs.getInt("PV");
				if(FunType.equals("2")){
					Result = rs.getString("Type");
					String[] volName = new String[] { "Date","OS","CTID","FunType","ObjectId","UV","PV","Result" };
					String[] volValue = new String[] { Date,OS, CTID, FunType,ObjectId, UV+"", PV+"", Result };
					String[] volConName = { "Date","OS", "CTID", "FunType","ObjectId", "Result" };
					String[] volConValue = { Date,OS, CTID, FunType, ObjectId,	Result };
					String[] Vname = new String[] { "UV", "PV" };
					String[] Vvalue = new String[] { UV + "", PV + "" };
					try {
						//超市秒杀Top分析
//						JobManager.ResultToDB(conn_da, "DA_SuperTop_Analysis", volConName, volConValue, volName, volValue, Vname, Vvalue);
						JobManager.DBToFile( "DA_SuperTop_Analysis", volConName, volConValue, volName, volValue, Vname, Vvalue,"");
		        	} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						MsgSender.sendMessage(new String[] {"err","Insert error\t"+Date+"\tSuperOrderBuyAnalyse\t"+e.getMessage()});
					}	
				}else if(FunType.equals("4")){
					String[] volName = new String[] { "Date","OS","CTID","FunType","ObjectId","UV","PV" };
					String[] volValue = new String[] { Date,OS, CTID, FunType,ObjectId, UV+"", PV+""};
					String[] volConName = { "Date","OS", "CTID", "FunType","ObjectId" };
					String[] volConValue = { Date,OS, CTID, FunType, ObjectId };
					String[] Vname = new String[] { "UV", "PV" };
					String[] Vvalue = new String[] { UV + "", PV + "" };
					try {
						//超市秒杀Top分析
//						JobManager.ResultToDB(conn_da, "DA_SuperTop_Analysis", volConName, volConValue, volName, volValue, Vname, Vvalue);
						JobManager.DBToFile( "DA_SuperTop_Analysis", volConName, volConValue, volName, volValue, Vname, Vvalue,"");
		        	} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						MsgSender.sendMessage(new String[] {"err","Insert error\t"+Date+"\tSuperOrderBuyAnalyse\t"+e.getMessage()});
					}	
				}		
			}
			rs.close();
			pstmt.close();
			conn_order.close();
			conn_da.close();
			conn_order=null;
			conn_da=null;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			MsgSender.sendMessage(new String[] {"err","Select error\t"+sql+"\tSuperOrderBuyAnalyse\t"+e.getMessage()});
			if (conn_order != null || conn_da != null) {
				try {
					conn_order.close();
					conn_da.close();
				} catch (Exception ee) {

				}
				conn_order = null;
				conn_da = null;
			}			
			return false;
		}
		return true;
	}
	public boolean OrderStatusAnalyse(String strDayN,String OrderType,String cityType){		
		String sql_success = "";   	  //购物成功
		String sql_cancel = "";       //取消订单
		String sql_confrim = "";      //确认收货
		String sql_sendTime = "";     //送货时间
		String sql_payMethod = ""; 	  //支付方式
		String sql_orderTotal = "";   //总订单量
		int flg = 0;
		if(OrderType.equals("1")){	  //底价秒杀
			if(cityType.equals("wholeCity")){		//不分城市
				sql_success = "SELECT count(distinct t.URID) as UV, count(distinct t.ORID) as PV FROM ("+
                              "SELECT DISTINCT o.ORID,o.URID FROM WCC_MSOrder o INNER JOIN WCC_MSOrderlists l on o.ORID=l.ORID "+
						      "where (l.Status in ('4','7','16','17')) and o.CreatedTime>=? and o.CreatedTime<=?) t";
			}else if(cityType.equals("city")){		//分城市
				sql_success = "SELECT CTID,count(distinct URID) as UV, count(distinct ORID) as PV from ("+
						      "SELECT o.ORID,o.URID,o.CTID FROM WCC_MSOrder o INNER JOIN WCC_MSOrderlists l on o.ORID=l.ORID where "+
						      "(l.Status in ('4','7','16','17')) and o.CreatedTime>=? and o.CreatedTime<=?) t group by t.CTID";
			}			
			flg += Fun_OrderStatusStatis(sql_success, strDayN, "LowSecKillBuySuccess", cityType);		//底价秒杀购物成功
			
			if(cityType.equals("wholeCity")){		//不分城市
				sql_cancel = "SELECT count(distinct URID) as UV, count(distinct ORID) as PV FROM ("+
						     "SELECT o.ORID,o.URID FROM WCC_MSOrder o INNER JOIN WCC_MSOrderlists l on o.ORID=l.ORID "+
						     "where (l.Status in ('5')) and o.CreatedTime>=? and o.CreatedTime<=?) t";
			}else if(cityType.equals("city")){		//分城市
				sql_cancel = "SELECT t.CTID,count(distinct t.URID) as UV, count(distinct t.ORID) as PV from ("+
							 "SELECT DISTINCT o.ORID,o.URID,o.CTID FROM WCC_MSOrder o INNER JOIN WCC_MSOrderlists l "+
						     "on o.ORID=l.ORID where (l.Status in ('5')) and o.CreatedTime>=? and o.CreatedTime<=?) t group by t.CTID;";
			}			
			flg += Fun_OrderStatusStatis(sql_cancel, strDayN, "LowSecKillCancelOrder", cityType); 	    //底价秒杀取消订单
			
			if(cityType.equals("wholeCity")){		//不分城市
				sql_payMethod = "SELECT PayMethod,count(distinct URID) as UV,count(distinct ORID) as PV FROM WCC_MSOrder "+
						        "where CreatedTime>=? and CreatedTime<=? group by PayMethod";
			}else if(cityType.equals("city")){		//分城市
				sql_payMethod = "SELECT CTID,PayMethod,count(distinct URID) as UV,count(distinct ORID) as PV FROM WCC_MSOrder "+
						        "where CreatedTime>=? and CreatedTime<=? group by CTID,PayMethod";
			}			
			flg += Fun_OrderStatusStatis(sql_payMethod, strDayN, "LowSecKillPay_", cityType); 			//底价秒杀支付方式	
			
			if(cityType.equals("wholeCity")){		//不分城市
				sql_orderTotal = "SELECT count(distinct URID) as UV, count(distinct ORID) as PV FROM WCC_MSOrder "+
					      	  "where CreatedTime>=? and CreatedTime<=? ";
			}else if(cityType.equals("city")){		//分城市
				sql_orderTotal = "SELECT CTID,count(distinct URID) as UV, count(distinct ORID) as PV FROM WCC_MSOrder "+
					          "where CreatedTime>=? and CreatedTime<=? group by CTID";
			}			
			flg += Fun_OrderStatusStatis(sql_orderTotal, strDayN, "LowSecKillOrderTotal", cityType);	//底价秒杀总订单量
		}else if(OrderType.equals("2")){//超市代购
			if(cityType.equals("wholeCity")){		//不分城市
				sql_success = "SELECT count(distinct t.URID) as UV, count(distinct t.ORID) as PV FROM ("+
                              "SELECT DISTINCT B.ORID,B.URID FROM WCC_BYOrder B INNER JOIN WCC_BYOrderHistory h "+
						      "on B.ORID=h.ORID where (h.Status in ('4','7','16','17')) and B.CreatedTime>=? and B.CreatedTime<=?) t;";
			}else if(cityType.equals("city")){		//分城市
				sql_success = "SELECT t.CTID,count(distinct t.URID) as UV, count(distinct t.ORID) as PV FROM ("+
						      "SELECT DISTINCT B.ORID,B.URID,S.CTID FROM WCC_BYOrder B INNER JOIN GC_Store S INNER JOIN WCC_BYOrderHistory h "+
						      "on B.STID=S.STID and B.ORID=h.ORID where (h.Status in ('4','7','16','17')) "+
						      "and B.CreatedTime>=? and B.CreatedTime<=?) t group by t.CTID";
			}			
			flg += Fun_OrderStatusStatis(sql_success, strDayN, "BYShoppingBuySuccess", cityType);		//超市代购购物成功
			
			if(cityType.equals("wholeCity")){		//不分城市
				sql_cancel = "SELECT count(distinct URID) as UV, count(*) as PV FROM ("+
							 "SELECT DISTINCT B.ORID,B.URID FROM WCC_BYOrder B INNER JOIN WCC_BYOrderHistory h on B.ORID=h.ORID "+
							 "where (h.Status in ('5')) and B.CreatedTime>=? and B.CreatedTime<=?) t;";
			}else if(cityType.equals("city")){		//分城市
				sql_cancel = "SELECT t.CTID,count(distinct t.URID) as UV, count(*) as PV FROM ("+
						     "SELECT DISTINCT B.ORID,B.URID,S.CTID FROM WCC_BYOrder B INNER JOIN GC_Store S INNER JOIN WCC_BYOrderHistory h "+
						     "on B.STID=S.STID and B.ORID=h.ORID where (h.Status in ('5')) and B.CreatedTime>=? and B.CreatedTime<=?) t group by t.CTID;";
			}			
			flg += Fun_OrderStatusStatis(sql_cancel, strDayN, "BYShoppingCancelOrder", cityType);	    //超市代购取消订单
			
			if(cityType.equals("wholeCity")){		//不分城市
				sql_confrim = "SELECT count(distinct h.URID) as UV, count(distinct h.ORID) as PV FROM "+
							  "WCC_BYOrderHistory h,WCC_BYOrder B,GC_Store S where h.ORID=B.ORID and B.STID=S.STID "+
							  "and B.Status='4' and h.CreatedTime>=? and h.CreatedTime<=?";
			}else if(cityType.equals("city")){		//分城市
				sql_confrim = "SELECT S.CTID,count(distinct h.URID) as UV, count(distinct h.ORID) as PV FROM "+
	                          "WCC_BYOrderHistory h,WCC_BYOrder B,GC_Store S where h.ORID=B.ORID and B.STID=S.STID "+
	                          "and B.Status='4' and h.CreatedTime>=? and h.CreatedTime<=? group by S.CTID";
			}			
			flg += Fun_OrderStatusStatis(sql_confrim, strDayN, "BYShoppingGoodsConfirm", cityType);  	//超市代购确认收货
			
			if(cityType.equals("wholeCity")){		//不分城市
				sql_sendTime = "select B.SendTime,count(distinct URID) as UV,count(distinct ORID) as PV from WCC_BYOrder B "+
						       "where B.CreatedTime>=? and B.CreatedTime<=? group by B.SendTime";
			}else if(cityType.equals("city")){		//分城市
				sql_sendTime = "select S.CTID,B.SendTime,count(distinct URID) as UV,count(distinct ORID) as PV from WCC_BYOrder B,GC_Store S "+
						       "where B.CreatedTime>=? and B.CreatedTime<=? and B.STID=S.STID group by B.SendTime,S.CTID";
			}			
			flg += Fun_OrderStatusStatis(sql_sendTime, strDayN, "BYShoppingSendTime_", cityType);	    //超市代购送货时间
			
			if(cityType.equals("wholeCity")){		//不分城市
				sql_orderTotal = "SELECT count(distinct URID) as UV, count(distinct ORID) as PV FROM WCC_BYOrder B "+
		                      "where B.CreatedTime>=? and B.CreatedTime<=? ";
			}else if(cityType.equals("city")){		//分城市
				sql_orderTotal = "SELECT S.CTID,count(distinct URID) as UV, count(distinct ORID) as PV FROM WCC_BYOrder B,"+
							  "GC_Store S where B.CreatedTime>=? and B.CreatedTime<=? and B.STID=S.STID group by S.CTID";
			}			
			flg += Fun_OrderStatusStatis(sql_orderTotal, strDayN, "BYShoppingOrderTotal", cityType);	//超市代购总订单量
		}else if(OrderType.equals("9")){	
			if(cityType.equals("wholeCity")){		//不分城市
				sql_orderTotal = "SELECT count(distinct UDID) as UV, count(distinct ID) as PV FROM "+
			          "WCC_Retail_Order where CreatedTime>=? and CreatedTime<=? and Tag=1 and Type=1 and State in (2,3,4,7)" ;
			}else if(cityType.equals("city")){	    //分城市
				sql_orderTotal = "SELECT CTID,count(distinct UDID) as UV, count(distinct ID) as PV FROM "+
			          "WCC_Retail_Order where CreatedTime>=? and CreatedTime<=? and Tag=1 and Type=1 and State in (2,3,4,7) group by CTID";
			}	
			flg += Fun_OrderStatusStatis(sql_orderTotal, strDayN, "FShoppingOrderOK", cityType);	//水果代购总订单量(有效)
			
			if(cityType.equals("wholeCity")){		//不分城市
				sql_orderTotal = "SELECT count(distinct UDID) as UV, count(distinct ID) as PV FROM "+
			          "WCC_Retail_Order where CreatedTime>=? and CreatedTime<=? and Tag=1 and Type=1 and (subString(Code,1,1)='S' or online=2)" ;
			}else if(cityType.equals("city")){	    //分城市
				sql_orderTotal = "SELECT CTID,count(distinct UDID) as UV, count(distinct ID) as PV FROM "+
			          "WCC_Retail_Order where CreatedTime>=? and CreatedTime<=? and Tag=1 and Type=1 and (subString(Code,1,1)='S' or online=2) group by CTID";
			}	
			flg += Fun_OrderStatusStatis(sql_orderTotal, strDayN, "FShoppingOrderPage", cityType);	//水果代购总订单量（网页下单）
			
			if(cityType.equals("wholeCity")){		//不分城市
				sql_orderTotal = "SELECT count(distinct UDID) as UV, count(distinct ID) as PV FROM "+
			          "WCC_Retail_Order where CreatedTime>=? and CreatedTime<=? and Tag=1 and Type=1 " ;
			}else if(cityType.equals("city")){	    //分城市
				sql_orderTotal = "SELECT CTID,count(distinct UDID) as UV, count(distinct ID) as PV FROM "+
			          "WCC_Retail_Order where CreatedTime>=? and CreatedTime<=? and Tag=1 and Type=1 group by CTID";
			}	
			flg += Fun_OrderStatusStatis(sql_orderTotal, strDayN, "FShoppingOrderTotal", cityType);	//水果代购总订单量（全部下单）
						
			if(cityType.equals("wholeCity")){		//不分城市
				sql_orderTotal = "SELECT count(distinct UDID) as UV, count(distinct ID) as PV FROM "+
	                    "WCC_Retail_Order where CreatedTime>=? and CreatedTime<=? and Tag=1 and Type=1 and State=5" ;
			}else if(cityType.equals("city")){	    //分城市
				sql_orderTotal = "SELECT CTID,count(distinct UDID) as UV, count(distinct ID) as PV FROM "+
			          "WCC_Retail_Order where CreatedTime>=? and CreatedTime<=? and Tag=1 and Type=1 and State=5 group by CTID";
			}	
			flg += Fun_OrderStatusStatis(sql_orderTotal, strDayN, "FShoppingOrderCancel", cityType);	//水果代购取消订单数
			
			if(cityType.equals("wholeCity")){		//不分城市
				sql_orderTotal = "SELECT count(distinct UDID) as UV, count(distinct ID) as PV FROM "+
	                    "WCC_Retail_Order where CreatedTime>=? and CreatedTime<=? and Tag=1 and Type=1 and IsSupported=2" ;
			}else if(cityType.equals("city")){	    //分城市
				sql_orderTotal = "SELECT CTID,count(distinct UDID) as UV, count(distinct ID) as PV FROM "+
			          "WCC_Retail_Order where CreatedTime>=? and CreatedTime<=? and Tag=1 and Type=1 and IsSupported=2 group by CTID";
			}	
			flg += Fun_OrderStatusStatis(sql_orderTotal, strDayN, "FShoppingOrderUnSupport", cityType);	//水果代购未开通城市订单
			
			if(cityType.equals("wholeCity")){		//不分城市
				sql_orderTotal = "SELECT subString(CreatedTime,12,2) as period ,count(distinct UDID) as UV, count(distinct ID) as PV FROM "+
			          "WCC_Retail_Order where CreatedTime>=? and CreatedTime<=? and Tag=1 and Type=1 group by subString(CreatedTime,12,2)" ;
			}else if(cityType.equals("city")){	    //分城市
				sql_orderTotal = "SELECT CTID,subString(CreatedTime,12,2) as period,count(distinct UDID) as UV, count(distinct ID) as PV FROM "+
			          "WCC_Retail_Order where CreatedTime>=? and CreatedTime<=? and Tag=1 and Type=1 group by CTID";
			}	
			flg += Fun_OrderStatusStatis(sql_orderTotal, strDayN, "FShoppingOrder_", cityType);	//水果代购时段订单（全部下单）
		}else if(OrderType.equals("11")){	
			if(cityType.equals("wholeCity")){		//不分城市
				sql_orderTotal = "SELECT count(distinct UDID) as UV, count(distinct ID) as PV FROM "+
			          "WCC_Retail_Order where CreatedTime>=? and CreatedTime<=? and Tag=2 and Type=1 and State in (2,3,4,7)" ;
			}else if(cityType.equals("city")){	    //分城市
				sql_orderTotal = "SELECT CTID,count(distinct UDID) as UV, count(distinct ID) as PV FROM "+
			          "WCC_Retail_Order where CreatedTime>=? and CreatedTime<=? and Tag=2 and Type=1 and State in (2,3,4,7) group by CTID";
			}	
			flg += Fun_OrderStatusStatis(sql_orderTotal, strDayN, "TShoppingOrderOK", cityType);	//特产代购总订单量(有效)
			
			if(cityType.equals("wholeCity")){		//不分城市
				sql_orderTotal = "SELECT count(distinct UDID) as UV, count(distinct ID) as PV FROM "+
			          "WCC_Retail_Order where CreatedTime>=? and CreatedTime<=? and Tag=2 and Type=1 and (subString(Code,1,1)='S' or online=2)" ;
			}else if(cityType.equals("city")){	    //分城市
				sql_orderTotal = "SELECT CTID,count(distinct UDID) as UV, count(distinct ID) as PV FROM "+
			          "WCC_Retail_Order where CreatedTime>=? and CreatedTime<=? and Tag=2 and Type=1 and (subString(Code,1,1)='S' or online=2) group by CTID";
			}	
			flg += Fun_OrderStatusStatis(sql_orderTotal, strDayN, "TShoppingOrderPage", cityType);	//特产代购总订单量（网页下单）
			
			if(cityType.equals("wholeCity")){		//不分城市
				sql_orderTotal = "SELECT count(distinct UDID) as UV, count(distinct ID) as PV FROM "+
			          "WCC_Retail_Order where CreatedTime>=? and CreatedTime<=? and Tag=2 and Type=1" ;
			}else if(cityType.equals("city")){	    //分城市
				sql_orderTotal = "SELECT CTID,count(distinct UDID) as UV, count(distinct ID) as PV FROM "+
			          "WCC_Retail_Order where CreatedTime>=? and CreatedTime<=? and Tag=2 and Type=1 group by CTID";
			}	
			flg += Fun_OrderStatusStatis(sql_orderTotal, strDayN, "TShoppingOrderTotal", cityType);	//特产代购总订单量（全部下单）
			
			if(cityType.equals("wholeCity")){		//不分城市
				sql_orderTotal = "SELECT count(distinct UDID) as UV, count(distinct ID) as PV FROM "+
			          "WCC_Retail_Order where CreatedTime>=? and CreatedTime<=? and Tag=2 and Type=1 and State=5" ;
			}else if(cityType.equals("city")){	    //分城市
				sql_orderTotal = "SELECT CTID,count(distinct UDID) as UV, count(distinct ID) as PV FROM "+
			          "WCC_Retail_Order where CreatedTime>=? and CreatedTime<=? and Tag=2 and Type=1 and State=5 group by CTID";
			}	
			flg += Fun_OrderStatusStatis(sql_orderTotal, strDayN, "TShoppingOrderCancel", cityType);	//特产代购取消订单
			
			if(cityType.equals("wholeCity")){		//不分城市
				sql_orderTotal = "SELECT count(distinct UDID) as UV, count(distinct ID) as PV FROM "+
			          "WCC_Retail_Order where CreatedTime>=? and CreatedTime<=? and Tag=2 and Type=1 and IsSupported=2" ;
			}else if(cityType.equals("city")){	    //分城市
				sql_orderTotal = "SELECT CTID,count(distinct UDID) as UV, count(distinct ID) as PV FROM "+
			          "WCC_Retail_Order where CreatedTime>=? and CreatedTime<=? and Tag=2 and Type=1 and IsSupported=2 group by CTID";
			}	
			flg += Fun_OrderStatusStatis(sql_orderTotal, strDayN, "TShoppingOrderUnSupport", cityType);	//特产代购未开通城市订单
		}
		if(flg == 0){
			return true;
		}else{
			return false;
		}
	}
	
	public int Fun_OrderStatusStatis(String sql,String strDayN,String Type,String cityType){
		String OS = "";
		String CTID = "";
		String sendTime = "";
		String payMethod = "";
		String period = "";
		String FunType = "";
		int UV = 0;
		int CPV = 0;
		PreparedStatement pstmt=null;
		ResultSet rs=null;
		OS=JobManager.OSdeal(OS);
		
		 if(conn_order == null){
			 conn_order = JobManager.getPriDao();
	   		 if(conn_order == null){
	   			 System.out.println("connection order failed!");
	   			 return 1;
	   		 }
		 } 	
		try {
			pstmt = conn_order.prepareStatement(sql);
			String Daytime1 = strDayN + " 00:00:00";
			String Daytime2 = strDayN + " 23:59:59";
			pstmt.setString(1, Daytime1);
			pstmt.setString(2, Daytime2);
			rs = pstmt.executeQuery();	
			
			if(conn_da == null){
				conn_da =  JobManager.getResultDao();
		   		 if(conn_da == null){
		   			 System.out.println("connection da failed!");
		   			 return 1;
		   		 }
			 } 
			
			while(rs.next()){	
				if(cityType.equals("wholeCity")){
					CTID = "4275";
				}else if(cityType.equals("city")){
					CTID = rs.getString("CTID");
					CTID=JobManager.CTIDdeal(CTID);
				}
				UV = rs.getInt("UV");
				CPV = rs.getInt("PV");	
				if(Type.equals("BYShoppingSendTime_")){
					sendTime = rs.getString("SendTime");
					FunType = "BYShoppingSendTime_"+sendTime;
				}else if(Type.equals("LowSecKillPay_")){
					payMethod = rs.getString("PayMethod");
					FunType = "LowSecKillPay_"+payMethod;
				}else if(Type.equals("FShoppingOrder_")){
					period = rs.getString("period");
					FunType = "FShoppingOrder_"+period;
				}else{
					FunType = Type;
				}
				String[] volName = new String[] { "Date","OS", "CTID", "FunType","CPV", "UV" };
				String[] volValue = new String[] { strDayN,OS, CTID, FunType,CPV+"", UV+"" };
				String[] volConName = { "Date","OS", "CTID", "FunType" };
				String[] volConValue = { strDayN,OS, CTID, FunType };
				String[] Vname = new String[] { "CPV", "UV" };
				String[] Vvalue = new String[] { CPV+"", UV+"" };
				try {
					//超市明细分析
//					JobManager.ResultToDB(conn_da, "DA_SuperDetail_Analysis", volConName, volConValue, volName, volValue, Vname, Vvalue);
					JobManager.DBToFile( "DA_SuperDetail_Analysis", volConName, volConValue, volName, volValue, Vname, Vvalue,"");
	        	} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					MsgSender.sendMessage(new String[] {"err","Insert error\t"+strDayN+"\tSuperOrderStatusAnalyse\t"+e.getMessage()});
				}				
			}
			rs.close();
			pstmt.close();
			conn_order.close();
			conn_da.close();
			conn_order=null;
			conn_da=null;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			MsgSender.sendMessage(new String[] {"err","Select error\t"+sql+"\tSuperOrderStatusAnalyse\t"+e.getMessage()});
			if (conn_order != null || conn_da != null) {
				try {
					conn_order.close();
					conn_da.close();
				} catch (Exception ee) {

				}
				conn_order = null;
				conn_da = null;
			}			
			return 1;
		}
		return 0;
	}
	
	public boolean Fun_FShoppingBuyCom(String strDayN,String cityType,String tag){
		 if(conn_order == null){
			 conn_order = JobManager.getPriDao();
	   		 if(conn_order == null){
	   			 System.out.println("connection order failed!");
	   			 return false;
	   		 }
		 } 	
		if (conn_da == null) {
			conn_da = JobManager.getResultDao();
			if (conn_da == null) {
				System.out.println("connection da failed!");
				return false;
			}
		}

		

		if(!tag.equals("1") && !tag.equals("2")){
			return false;
		}

		String Date = strDayN;
		String OS = "";
		String CTID = "";
		String Spec = "";
		int ObjectId = 0;		
		float Multiply = 0.0f;
		float Amount = 0.0f;
		int Number = 0;	
		int UV = 0;
		String sql = "";
		String tablename = "";
		if(tag.equals("1")){
			tablename="DA_FShopCom_Analysis";
		}else if(tag.equals("2")){
			tablename="DA_TShopCom_Analysis";
		}
		OS = JobManager.OSdeal(OS);		
		if(cityType.equals("wholeCity")){   	//不分城市
			sql = "SELECT d.CommodityID,d.Spec,d.Multiply,sum(d.Number) as Number,d.Price*sum(d.Number) as amount,count(o.UDID) as uv "+
				  "FROM WCC_Retail_Order o LEFT JOIN WCC_Retail_Order_Detail d on o.ID=d.OrderID where o.CreatedTime>=? and o.CreatedTime<=? "+
				  "and o.Tag=? and o.Type=1 and o.State in (2,3,4,7) GROUP BY d.CommodityID,d.Spec";
		} else if (cityType.equals("city")) {   // 分城市
			sql = "SELECT o.CTID,d.CommodityID,d.Spec,d.Multiply,sum(d.Number) as Number,d.Price*sum(d.Number) as amount,count(o.UDID) as uv "+  
				  "FROM WCC_Retail_Order o LEFT JOIN WCC_Retail_Order_Detail d on o.ID=d.OrderID where o.CreatedTime>=? and o.CreatedTime<=? "+
				  "and o.Tag=? and o.Type=1 and o.State in (2,3,4,7) GROUP BY o.CTID,d.CommodityID,d.Spec";
		}	
		PreparedStatement pstmt;
		ResultSet rs;
		try {
			pstmt = conn_order.prepareStatement(sql);
			String Daytime1 = strDayN + " 00:00:00";
			String Daytime2 = strDayN + " 23:59:59";
			pstmt.setString(1, Daytime1);
			pstmt.setString(2, Daytime2);
			pstmt.setString(3, tag);
			rs = pstmt.executeQuery();				
			while(rs.next()){	
				if(cityType.equals("wholeCity")){
					CTID = "4275";
				}else if(cityType.equals("city")){
					CTID = rs.getString("CTID");
					CTID = JobManager.CTIDdeal(CTID);
				}
				ObjectId = rs.getInt("CommodityID");
				Spec = rs.getString("Spec");
				Multiply = rs.getFloat("Multiply");
				Number = rs.getInt("Number");	
				Amount = rs.getFloat("amount");
				UV = rs.getInt("uv");
				
				String[] volName = new String[] { "Date","OS","CTID","ObjectId","Spec","Multiply","Number","Amount","UV" };
				String[] volValue = new String[] { Date,OS, CTID, ObjectId+"",Spec,Multiply+"",Number+"",Amount+"",UV+""  };
				String[] volConName = { "Date","OS", "CTID", "ObjectId","Spec" };
				String[] volConValue = { Date,OS, CTID, ObjectId+"",Spec};
				String[] Vname = new String[] { "Multiply","Number","Amount","UV"  };
				String[] Vvalue = new String[] { Multiply+"",Number+"",Amount+"",UV+"" };
				try {
					//水果代购 水果购买
//					JobManager.ResultToDB(conn_da, tablename, volConName, volConValue, volName, volValue, Vname, Vvalue);
					JobManager.DBToFile( tablename, volConName, volConValue, volName, volValue, Vname, Vvalue,"");
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					MsgSender.sendMessage(new String[] {"err","Insert error\t"+Date+"\tSuperOrderBuyAnalyse\t"+e.getMessage()});
				}
				
			}			
			rs.close();
			pstmt.close();
			conn_order.close();
			conn_da.close();
			conn_order=null;
			conn_da=null;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			MsgSender.sendMessage(new String[] {"err","Select error\t"+sql+"\tSuperOrderBuyAnalyse\t"+e.getMessage()});
			if (conn_order != null || conn_da != null) {
				try {
					conn_order.close();
					conn_da.close();
				} catch (Exception ee) {

				}
				conn_order = null;
				conn_da = null;
			}			
			return false;
		}
		return true;
	}	
}
