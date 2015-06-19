package com.wochacha.da.transform.superSeckill;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.wochacha.da.model.JobManager;
import com.wochacha.da.model.Record;

public class SuperSeckillMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	public Connection conn_order = null;
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
		String Message = arr[11];
		String recordFlag = arr[12];  //1:来自user 2:来自client
		String Remarks = arr[14];
		String CTID_whole="4275";     //全国		
		try {
			if(!(Action.equalsIgnoreCase("click.index")&&DirectObject.equalsIgnoreCase("Supermarket"))){
				//分城市
				output.collect(new Text("SupmarketAll"+"\t"+OS+"\t"+CTID), new Text(UDID));		
				//全国
				output.collect(new Text("SupmarketAll"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));
			}
			if(Action.equalsIgnoreCase("Switch.LowSecKill") && DirectObject.equalsIgnoreCase("Supermarket")){      //一级页面--底价秒杀
				//分城市
				output.collect(new Text("LowSecKill"+"\t"+OS+"\t"+CTID), new Text(UDID));		
				//全国
				output.collect(new Text("LowSecKill"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));
				
			}else if(Action.equalsIgnoreCase("Switch.BYShopping") && DirectObject.equalsIgnoreCase("BYShopping")  //一级页面--超市代购
					&& recordFlag.equals("2")){  //进入超市代购页面这条记录会同时被User和Client记录，故这里只取Client
				//分城市
				output.collect(new Text("BYShopping"+"\t"+OS+"\t"+CTID), new Text(UDID));		
				//全国
				output.collect(new Text("BYShopping"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));
				
			}else if(Action.equalsIgnoreCase("Switch.Poster") && DirectObject.equalsIgnoreCase("Supermarket")){	   //一级页面--超市海报
				//分城市
				output.collect(new Text("Poster"+"\t"+OS+"\t"+CTID), new Text(UDID));			
				//全国
				output.collect(new Text("Poster"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));	
				
			}else if(Action.equalsIgnoreCase("Switch.Search") && DirectObject.equalsIgnoreCase("Supermarket")){    //一级页面--找超市
				//分城市
				output.collect(new Text("Search"+"\t"+OS+"\t"+CTID), new Text(UDID));			
				//全国
				output.collect(new Text("Search"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));	
				
			}else if(Action.equalsIgnoreCase("Access.Promotion") && DirectObject.equalsIgnoreCase("Supermarket")){ //二级页面--底价秒杀商品详情
				//分城市
				output.collect(new Text("LowSecKillPromotion"+"\t"+OS+"\t"+CTID), new Text(UDID));			
				//全国
				output.collect(new Text("LowSecKillPromotion"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));	
				
				String PKID = IndirectObject;                                                					  //Top分析--底价秒杀商品浏览
				
				//分城市
				output.collect(new Text("LowSecKill_SkimPromotion"+"\t"+OS+"\t"+CTID+"\t"+PKID), new Text(UDID));			
				//全国
				output.collect(new Text("LowSecKill_SkimPromotion"+"\t"+OS+"\t"+CTID_whole+"\t"+PKID), new Text(UDID));	
				
			}else if(Action.equalsIgnoreCase("Switch.Promotion") && DirectObject.equalsIgnoreCase("BYShopping")){  //二级页面--超市代购商品详情
				//分城市
				output.collect(new Text("BYShoppingPromotion"+"\t"+OS+"\t"+CTID), new Text(UDID));			
				//全国
				output.collect(new Text("BYShoppingPromotion"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));	
				
				String bprid = IndirectObject;                                                                     //Top分析--超市代购商品浏览
				
				//分城市
				output.collect(new Text("BYShopping_SkimPromotion"+"\t"+OS+"\t"+CTID+"\t"+bprid), new Text(UDID));			
				//全国
				output.collect(new Text("BYShopping_SkimPromotion"+"\t"+OS+"\t"+CTID_whole+"\t"+bprid), new Text(UDID));	
								
			}else if (Action.equalsIgnoreCase("Access.Sup") && DirectObject.equalsIgnoreCase("Supermarket")        // 二级页面--秒杀超市切换(用client数据)
					&& recordFlag.equalsIgnoreCase("2")) {
				//分城市
				output.collect(new Text("LowSecKillSupMSwitch"+"\t"+OS+"\t"+CTID), new Text(UDID));			
				//全国
				output.collect(new Text("LowSecKillSupMSwitch"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));	

			}else if(Action.equalsIgnoreCase("Search.com") && DirectObject.equalsIgnoreCase("BYShopping")){       //二级页面--搜索商品
				//分城市
				output.collect(new Text("BYShoppingSearchCom"+"\t"+OS+"\t"+CTID), new Text(UDID));					
				//全国
				output.collect(new Text("BYShoppingSearchCom"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));	
				//搜索商品 （统计有结果的搜索 ） Result=0有结果,Result=1无结果
				if(Result.equalsIgnoreCase("0")){
					//分城市
					output.collect(new Text("KeySearchReSearchCom"+"\t"+OS+"\t"+CTID), new Text(UDID));					
					//全国
					output.collect(new Text("KeySearchReSearchCom"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));		
				}				
			}else if((Action.equalsIgnoreCase("Search.Sup") && DirectObject.equalsIgnoreCase("Supermarket")) || 
					(Action.equalsIgnoreCase("Search.Sup") && DirectObject.equalsIgnoreCase("SupermarketPoster")) ||
					(Action.equalsIgnoreCase("Search.Sup") && DirectObject.equalsIgnoreCase("SupermarketFind"))){ //搜索超市（促销海报和找超市）
				//说明：Action=search.Sup DirectObject=Supermarket是原始搜索超市参数，不区分是促销海报还是找超市。
				//后来DirectObject又以SupermarketPoster和SupermarketFind来区分促销海报和找超市
				
				//分城市
				output.collect(new Text("SearchSupM"+"\t"+OS+"\t"+CTID), new Text(UDID));			
				//全国
				output.collect(new Text("SearchSupM"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));	
				//搜索超市（统计有结果的搜索 ） Result=1有结果,Result=0无结果  
				if(Result.equalsIgnoreCase("1")){
					//分城市
					output.collect(new Text("KeySearchReSearchSupM"+"\t"+OS+"\t"+CTID), new Text(UDID));					
					//全国
					output.collect(new Text("KeySearchReSearchSupM"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));	
					
					String keyWord = IndirectObject.toLowerCase();
					//分城市
					output.collect(new Text("SearchSuperM"+"\t"+OS+"\t"+CTID+"\t"+keyWord+"_1"), new Text(UDID));			
					//全国
					output.collect(new Text("SearchSuperM"+"\t"+OS+"\t"+CTID_whole+"\t"+keyWord+"_1"), new Text(UDID));	
				}else if(Result.equalsIgnoreCase("0")){
					String keyWord = IndirectObject.toLowerCase();
					//分城市
					output.collect(new Text("SearchSuperM"+"\t"+OS+"\t"+CTID+"\t"+keyWord+"_0"), new Text(UDID));			
					//全国
					output.collect(new Text("SearchSuperM"+"\t"+OS+"\t"+CTID_whole+"\t"+keyWord+"_0"), new Text(UDID));	
				}
				
				if(DirectObject.equalsIgnoreCase("SupermarketPoster")){           //海报搜索超市
					//分城市
					output.collect(new Text("SearchSupM_poster"+"\t"+OS+"\t"+CTID), new Text(UDID));			
					//全国
					output.collect(new Text("SearchSupM_poster"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));	
				}else if(DirectObject.equalsIgnoreCase("SupermarketFind")){      //附近超市搜索超市
					//分城市
					output.collect(new Text("SearchSupM_find"+"\t"+OS+"\t"+CTID), new Text(UDID));			
					//全国
					output.collect(new Text("SearchSupM_find"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));	
				}
			}else if(Action.equalsIgnoreCase("Click.quiscan") && DirectObject.equalsIgnoreCase("BYShopping")){    //购物车入口--连续扫描入口
				//分城市
				output.collect(new Text("InCartQuiscan"+"\t"+OS+"\t"+CTID), new Text(UDID));			
				//全国
				output.collect(new Text("InCartQuiscan"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));	
				
			}else if(Action.equalsIgnoreCase("Add.Cart") && DirectObject.equalsIgnoreCase("Barcode")){      	  //购物车入口--比价结果入口
				//分城市
				output.collect(new Text("InCartCompPri"+"\t"+OS+"\t"+CTID), new Text(UDID));			
				//全国
				output.collect(new Text("InCartCompPri"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));	
				
			}else if(Action.equalsIgnoreCase("Add.Cart") && DirectObject.equalsIgnoreCase("BYShopping")){      	  //购物车入口--商品详情入口
				//分城市
				output.collect(new Text("InCartComDetail"+"\t"+OS+"\t"+CTID), new Text(UDID));			
				//全国
				output.collect(new Text("InCartComDetail"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));	
				
			}else if(Action.equalsIgnoreCase("Add.Cart") && DirectObject.equalsIgnoreCase("profile")){      	  //购物车入口--订单管理入口
				//分城市
				output.collect(new Text("InCartOrderMan"+"\t"+OS+"\t"+CTID), new Text(UDID));			
				//全国
				output.collect(new Text("InCartOrderMan"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));	
				
			}else if(Action.equalsIgnoreCase("Choice.classify") && DirectObject.equalsIgnoreCase("BYShopping")){  //各分类详情页！！！！！！！！还没做
				String CLID = IndirectObject;
				String CatId1="";				
				int i=0;
				while(i<3){
					if(conn_order==null){
//						ConnectToDb();
						conn_order = JobManager.getPriDao();
						if(conn_order!=null){
							break;
						}
					}
					i++;
				}				
				CatId1=queryCatId1(CLID);				
				//分城市
				output.collect(new Text("ClassifyPage_"+CatId1+"\t"+OS+"\t"+CTID), new Text(UDID));			
				//全国
				output.collect(new Text("ClassifyPage_"+CatId1+"\t"+OS+"\t"+CTID_whole), new Text(UDID));
			}else if(Action.equalsIgnoreCase("Click.Estate") && DirectObject.equalsIgnoreCase("Supermarket")){    // 底价秒杀--选择小区
				//分城市
				output.collect(new Text("LowSecKillSelectEstate"+"\t"+OS+"\t"+CTID), new Text(UDID));			
				//全国
				output.collect(new Text("LowSecKillSelectEstate"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));	
			}else if ((Action.equalsIgnoreCase("Click.SecKill") || Action.equalsIgnoreCase("Click.Purchase"))     // 底价秒杀--立即秒杀/抢购
					&& DirectObject.equalsIgnoreCase("SuperSedkill")) { 
				//分城市
				output.collect(new Text("LowSecKillSKP"+"\t"+OS+"\t"+CTID), new Text(UDID));			
				//全国
				output.collect(new Text("LowSecKillSKP"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));	
			}else if ((Action.equalsIgnoreCase("Click.Subsed") || Action.equalsIgnoreCase("Click.Subbuy"))        // 底价秒杀--秒杀/抢购提交订单
					&& DirectObject.equalsIgnoreCase("Supermarket") && Result.equals("1")) {//1表示提交成功的订单 
				//分城市				
				output.collect(new Text("LowSecKillSubOrder"+"\t"+OS+"\t"+CTID), new Text(UDID));			
				//全国
				output.collect(new Text("LowSecKillSubOrder"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));	
			}else if(Action.equalsIgnoreCase("Click.Payment") && DirectObject.equalsIgnoreCase("SuperSedkill")){  //底价秒杀--在线支付
				//分城市
				output.collect(new Text("LowSecKillPayOnline"+"\t"+OS+"\t"+CTID), new Text(UDID));			
				//全国
				output.collect(new Text("LowSecKillPayOnline"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));					
			}else if(Action.equalsIgnoreCase("click.store") && DirectObject.equalsIgnoreCase("BYShopping")){      //超市代购--选择超市门店
				//分城市
				output.collect(new Text("BYShoppingSelectStore"+"\t"+OS+"\t"+CTID), new Text(UDID));			
				//全国
				output.collect(new Text("BYShoppingSelectStore"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));					
			}else if(Action.equalsIgnoreCase("click.compare") && DirectObject.equalsIgnoreCase("BYShopping")){    //超市代购--比价结算
				//分城市
				output.collect(new Text("BYShoppingCompPriCheck"+"\t"+OS+"\t"+CTID), new Text(UDID));			
				//全国
				output.collect(new Text("BYShoppingCompPriCheck"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));				
			}else if((Action.equalsIgnoreCase("click.checkout") && DirectObject.equalsIgnoreCase("BYShopping")
					|| (Action.equalsIgnoreCase("click.checkout") && DirectObject.startsWith("car")
							&& IndirectObject.equals("1")))){                                                    //超市代购--去结算
				//分城市
				output.collect(new Text("BYShoppingCheckOut"+"\t"+OS+"\t"+CTID), new Text(UDID));			
				//全国
				output.collect(new Text("BYShoppingCheckOut"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));				
			}else if(Action.equalsIgnoreCase("Click.SubOrder") && DirectObject.equalsIgnoreCase("BYShopping")     //超市代购--提交订单
					&& Result.equals("1")){   //1表示提交成功的订单
				//分城市
				output.collect(new Text("BYShoppingSubOrder"+"\t"+OS+"\t"+CTID), new Text(UDID));			
				//全国
				output.collect(new Text("BYShoppingSubOrder"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));				
//			}else if(Action.equalsIgnoreCase("Click.Poster") && DirectObject.equalsIgnoreCase("Supermarket")){    //促销海报--海报浏览
			}else if(Action.equalsIgnoreCase("show.Poster") && DirectObject.equalsIgnoreCase("Supermarket")){ 
				String PPID = IndirectObject;
				//分城市
				output.collect(new Text("Poster_SkimPoster"+"\t"+OS+"\t"+CTID+"\t"+PPID), new Text(UDID));			
				//全国
				output.collect(new Text("Poster_SkimPoster"+"\t"+OS+"\t"+CTID_whole+"\t"+PPID), new Text(UDID));	
				                                                                                                    
				//分城市
				output.collect(new Text("PosterSkimNum"+"\t"+OS+"\t"+CTID), new Text(UDID));			
				//全国
				output.collect(new Text("PosterSkimNum"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));	
				
				//分城市                                                                                                                                                                                    //城市海报pv uv SKU
				output.collect(new Text("Shopping_Poster"+"\t"+OS+"\t"+CTID+"\t"+CTID), new Text(UDID+"\t"+PPID));			
				//全国
				output.collect(new Text("Shopping_Poster"+"\t"+OS+"\t"+CTID_whole+"\t"+CTID_whole), new Text(UDID+"\t"+PPID));	
			}else if(Action.equalsIgnoreCase("click.store") && DirectObject.equalsIgnoreCase("BYShopping")){      //超市代购--门店选择
				String STID = IndirectObject;
				//分城市
				output.collect(new Text("BYShopping_SelectStore"+"\t"+OS+"\t"+CTID+"\t"+STID), new Text(UDID));			
				//全国
				output.collect(new Text("BYShopping_SelectStore"+"\t"+OS+"\t"+CTID_whole+"\t"+STID), new Text(UDID));				
			}else if(Action.equalsIgnoreCase("switch.shopsale") && DirectObject.equalsIgnoreCase("supermarket")){ //切换超市促销
				//分城市
				output.collect(new Text("SupShopsale"+"\t"+OS+"\t"+CTID), new Text(UDID));			
				//全国
				output.collect(new Text("SupShopsale"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));				
			}else if(Action.equalsIgnoreCase("switch.list") && DirectObject.equalsIgnoreCase("supermarket")){     //切换超市清单
				//分城市
				output.collect(new Text("SupList"+"\t"+OS+"\t"+CTID), new Text(UDID));			
				//全国
				output.collect(new Text("SupList"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));				
			}else if(Action.equalsIgnoreCase("switch.bill") && DirectObject.equalsIgnoreCase("supermarket")){     //切换超市账单
				//分城市
				output.collect(new Text("SupBill"+"\t"+OS+"\t"+CTID), new Text(UDID));			
				//全国
				output.collect(new Text("SupBill"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));				
			}else if(Action.equalsIgnoreCase("Access.PromotionDetail") && DirectObject.equalsIgnoreCase("supermarket")){ //底价秒杀- 进入底价秒杀商品详情页
				//分城市
				output.collect(new Text("LowSecKillPromotionDetail"+"\t"+OS+"\t"+CTID), new Text(UDID));			
				//全国
				output.collect(new Text("LowSecKillPromotionDetail"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));			
			}else if(Action.equalsIgnoreCase("Switch.PromotionDetail") && DirectObject.equalsIgnoreCase("BYShopping")){ //超市代购- 进入超市代购商品详情页
				//分城市
				output.collect(new Text("BYShoppingPromotionDetail"+"\t"+OS+"\t"+CTID), new Text(UDID));			
				//全国
				output.collect(new Text("BYShoppingPromotionDetail"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));			
			}else if(Action.equalsIgnoreCase("click.activedetail") && DirectObject.equalsIgnoreCase("supermarket")){    //超市促销 - 促销活动访问
				String paid = IndirectObject;
				output.collect(new Text("ShoppingActive"+"\t"+OS+"\t"+CTID), new Text(UDID));			
				output.collect(new Text("ShoppingActive"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));	
				
				output.collect(new Text("Shopping_active"+"\t"+OS+"\t"+CTID+"\t"+paid), new Text(UDID));			
				output.collect(new Text("Shopping_active"+"\t"+OS+"\t"+CTID_whole+"\t"+paid), new Text(UDID));	
			}else if(Action.equalsIgnoreCase("click.favpro") && DirectObject.equalsIgnoreCase("supermarket")&& recordFlag.equals("1")){    //超市促销 - 收藏促销商品
				String PKID = IndirectObject;    
				
				if(Result.equalsIgnoreCase("1")){//从超市促销首页进入
					output.collect(new Text("ProFavIndex"+"\t"+OS+"\t"+CTID), new Text(UDID));			
					output.collect(new Text("ProFavIndex"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));	
									
				}else if(Result.equalsIgnoreCase("2")){//从列表页进入
					output.collect(new Text("ProFavList"+"\t"+OS+"\t"+CTID), new Text(UDID));			
					output.collect(new Text("ProFavList"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));		
					
				}
				
			}else if(Action.equalsIgnoreCase("cancel.favpro") && DirectObject.equalsIgnoreCase("supermarket")&& recordFlag.equals("1")){    //超市促销 - 取消收藏促销商品
				String PKID = IndirectObject;    
				if(Result.equalsIgnoreCase("1")){//从超市促销首页进入
					output.collect(new Text("ProNotFavIndex"+"\t"+OS+"\t"+CTID), new Text(UDID));			
					output.collect(new Text("ProNotFavIndex"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));		
					
				}else if(Result.equalsIgnoreCase("2")){//从列表页进入
					output.collect(new Text("ProNotFavList"+"\t"+OS+"\t"+CTID), new Text(UDID));			
					output.collect(new Text("ProNotFavList"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));		
					
				}
			}else if(Action.equalsIgnoreCase("click.price") && DirectObject.equalsIgnoreCase("supermarket")&& recordFlag.equals("2")){    //超市促销 - 点击比价按钮
				output.collect(new Text("PriceCompare"+"\t"+OS+"\t"+CTID), new Text(UDID));			
				output.collect(new Text("PriceCompare"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));	
				
			}else if(Action.equalsIgnoreCase("click.storelist") && DirectObject.equalsIgnoreCase("supermarket")&& recordFlag.equals("2")){    //超市促销 - 点击进入超市门店列表页
				String PKID = IndirectObject;   
				output.collect(new Text("StoreList"+"\t"+OS+"\t"+CTID), new Text(UDID));			
				output.collect(new Text("StoreList"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));	
				
			}else if(Action.equalsIgnoreCase("click.searchbox") && DirectObject.equalsIgnoreCase("supermarket")&& recordFlag.equals("2")){    //超市促销 - 点击搜索框
				output.collect(new Text("SearchBox"+"\t"+OS+"\t"+CTID), new Text(UDID));			
				output.collect(new Text("SearchBox"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));	
				
			}  else if ((Action.equalsIgnoreCase("click.MinpriceSup")
					&& DirectObject.equalsIgnoreCase("supermarket") && recordFlag.equals("2"))
					|| (Action.equalsIgnoreCase("show.activedetail")
							&& DirectObject.equalsIgnoreCase("supermarket") && recordFlag.equals("2"))) {              // 超市促销 - 全城最低价 - 点击进入推荐超市
				String[] tmp = null;
				String BRID = "";								
				if (Action.equalsIgnoreCase("click.MinpriceSup")) {
					if (IndirectObject.indexOf(",") >= 0) {
						tmp = IndirectObject.split(",");
					} else if (IndirectObject.indexOf("，") >= 0) {
						tmp = IndirectObject.split("，");
					}
					if (tmp.length == 2) {
						BRID = tmp[1];
					}
				} else if (Action.equalsIgnoreCase("show.activedetail")) {
					BRID = IndirectObject;
				}
				output.collect(new Text("MinPriClickSup"+"\t"+OS+"\t"+CTID), new Text(UDID));			
			    output.collect(new Text("MinPriClickSup"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));	
				
				output.collect(new Text("MinPri_Sup"+"\t"+OS+"\t"+CTID+"\t"+BRID), new Text(UDID));			
				output.collect(new Text("MinPri_Sup"+"\t"+OS+"\t"+CTID_whole+"\t"+BRID), new Text(UDID));	
			} else if (Action.equalsIgnoreCase("compare.price")	&& DirectObject.equalsIgnoreCase("SuperSedKill")
					&& recordFlag.equals("1")) {                                                                         // 全城最低价商品浏览
				String[] tmp = null;
				String PKID = "";
				String STID = "";
				if (IndirectObject.indexOf(",") >= 0) {
					tmp = IndirectObject.split(",");
				} else if (IndirectObject.indexOf("，") >= 0) {
					tmp = IndirectObject.split("，");
				}
				if (tmp.length == 2) {
					PKID = tmp[0];
					STID = tmp[1];
				}
				output.collect(new Text("MinPriComparePri"+"\t"+OS+"\t"+CTID), new Text(UDID));	
				output.collect(new Text("MinPriComparePri"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));	
				
				output.collect(new Text("MinPri_SkimCom"+"\t"+OS+"\t"+CTID+"\t"+PKID+"_"+STID), new Text(UDID));			
				output.collect(new Text("MinPri_SkimCom"+"\t"+OS+"\t"+CTID_whole+"\t"+PKID+"_"+STID), new Text(UDID));	
			}else if(Action.equalsIgnoreCase("Switch.MinPriceTab")	&& DirectObject.equalsIgnoreCase("Supermarket")){ //切换分类tab
				String[] tmp=null;
				String tabid = "";
				if (IndirectObject.indexOf(",") >= 0) {
					tmp = IndirectObject.split(",");
				} else if (IndirectObject.indexOf("，") >= 0) {
					tmp = IndirectObject.split("，");
				}
				if(tmp.length==2){
					tabid=tmp[0];
					//分城市
					output.collect(new Text("MinPriTab_"+tabid+"\t"+OS+"\t"+CTID), new Text(UDID));			
					//全国
					output.collect(new Text("MinPriTab_"+tabid+"\t"+OS+"\t"+CTID_whole), new Text(UDID));
				}
			}else if(Action.equalsIgnoreCase("switch.fruit")&&DirectObject.equalsIgnoreCase("supermarket")){ //水果代购 -- 进入水果代购页面
				if(Remarks.equalsIgnoreCase("wap")){
					//分城市
					output.collect(new Text("FruitShop_wap"+"\t"+OS+"\t"+CTID), new Text(UDID));			
					//全国
					output.collect(new Text("FruitShop_wap"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));
				}else{
					//分城市
					output.collect(new Text("FruitShopping"+"\t"+OS+"\t"+CTID), new Text(UDID));			
					//全国
					output.collect(new Text("FruitShopping"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));
				}
			}else if(Action.equalsIgnoreCase("switch.tab")&&DirectObject.equalsIgnoreCase("supermarketfruit")){//水果代购 -- 切换水果分类tab
				output.collect(new Text("FShoppingSwitchTab"+"\t"+OS+"\t"+CTID), new Text(UDID));			
				output.collect(new Text("FShoppingSwitchTab"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));
				
				String clid=IndirectObject;
				output.collect(new Text("FShopping_Topic"+"\t"+OS+"\t"+CTID+"\t"+clid), new Text(UDID));	
				output.collect(new Text("FShopping_Topic"+"\t"+OS+"\t"+CTID_whole+"\t"+clid), new Text(UDID));
			}else if(Action.equalsIgnoreCase("show.detail")&&DirectObject.equalsIgnoreCase("supermarketfruit")){//水果代购 -- 商品浏览
				if(Remarks.equalsIgnoreCase("wap")){
					output.collect(new Text("FshopComSkim_wap"+"\t"+OS+"\t"+CTID), new Text(UDID));			
					output.collect(new Text("FshopComSkim_wap"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));
				}else{
					output.collect(new Text("FShoppingComSkim"+"\t"+OS+"\t"+CTID), new Text(UDID));			
					output.collect(new Text("FShoppingComSkim"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));
				}				
				
				String frid=IndirectObject;
				output.collect(new Text("FShopping_SkimCom"+"\t"+OS+"\t"+CTID+"\t"+frid), new Text(UDID));	
				output.collect(new Text("FShopping_SkimCom"+"\t"+OS+"\t"+CTID_whole+"\t"+frid), new Text(UDID));
				
			}else if(Action.equalsIgnoreCase("click.buy")&&DirectObject.equalsIgnoreCase("supermarketfruit")){//水果代购 -- 立即购买
				if(Remarks.equalsIgnoreCase("wap")){
					output.collect(new Text("FShopBuyNow_wap"+"\t"+OS+"\t"+CTID), new Text(UDID));		
					output.collect(new Text("FShopBuyNow_wap"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));
				}else{
					output.collect(new Text("FShoppingBuyNow"+"\t"+OS+"\t"+CTID), new Text(UDID));		
					output.collect(new Text("FShoppingBuyNow"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));
				}				
			}else if(Action.equalsIgnoreCase("switch.Cart")&&DirectObject.equalsIgnoreCase("supermarketfruit")){//水果代购 -- 进入水果篮
				if(Remarks.equalsIgnoreCase("wap")){
					output.collect(new Text("FShopBasket_wap"+"\t"+OS+"\t"+CTID), new Text(UDID));			
					output.collect(new Text("FShopBasket_wap"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));
				}else{
					output.collect(new Text("FShoppingBasket"+"\t"+OS+"\t"+CTID), new Text(UDID));			
					output.collect(new Text("FShoppingBasket"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));
				}				
			}else if(Action.equalsIgnoreCase("click.subsed")&&DirectObject.equalsIgnoreCase("supermarketfruit")){//水果代购 -- 确认订单
				if(Remarks.equalsIgnoreCase("wap")){
					output.collect(new Text("FShopOrderConfirm_wap"+"\t"+OS+"\t"+CTID), new Text(UDID));			
					output.collect(new Text("FShopOrderConfirm_wap"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));
				}else{
					output.collect(new Text("FShoppingOrderConfirm"+"\t"+OS+"\t"+CTID), new Text(UDID));			
					output.collect(new Text("FShoppingOrderConfirm"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));
				}				
			}else if (Action.equalsIgnoreCase("shake") && DirectObject.equalsIgnoreCase("Index") &&     //促销摇一摇	
					IndirectObject.equals("2")) { 
				output.collect(new Text("ShakePromotion"+"\t"+OS+"\t"+CTID), new Text(UDID));			
				output.collect(new Text("ShakePromotion"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));
			}else if (Action.equalsIgnoreCase("shake") && DirectObject.equalsIgnoreCase("Index") &&     //水果代购摇一摇	
					IndirectObject.equals("3")) {  
				output.collect(new Text("ShakeFShopping"+"\t"+OS+"\t"+CTID), new Text(UDID));			
				output.collect(new Text("ShakeFShopping"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));
			
			}else if(Action.equalsIgnoreCase("switch.Native")&&DirectObject.equalsIgnoreCase("supermarket")){ //特产代购 -- 进入水果代购页面
				//分城市
				output.collect(new Text("TShopping"+"\t"+OS+"\t"+CTID), new Text(UDID));			
				//全国
				output.collect(new Text("TShopping"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));
			}else if(Action.equalsIgnoreCase("switch.tab")&&DirectObject.equalsIgnoreCase("supermarketNative")){//特产代购 -- 切换水果分类tab
				output.collect(new Text("TShoppingSwitchTab"+"\t"+OS+"\t"+CTID), new Text(UDID));			
				output.collect(new Text("TShoppingSwitchTab"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));
				
				String clid=IndirectObject;
				output.collect(new Text("TShopping_Topic"+"\t"+OS+"\t"+CTID+"\t"+clid), new Text(UDID));	
				output.collect(new Text("TShopping_Topic"+"\t"+OS+"\t"+CTID_whole+"\t"+clid), new Text(UDID));
			}else if(Action.equalsIgnoreCase("show.detail")&&DirectObject.equalsIgnoreCase("supermarketNative")){//特产代购 -- 商品浏览
				output.collect(new Text("TShoppingComSkim"+"\t"+OS+"\t"+CTID), new Text(UDID));			
				output.collect(new Text("TShoppingComSkim"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));
				
				String frid=IndirectObject;
				output.collect(new Text("TShopping_SkimCom"+"\t"+OS+"\t"+CTID+"\t"+frid), new Text(UDID));	
				output.collect(new Text("TShopping_SkimCom"+"\t"+OS+"\t"+CTID_whole+"\t"+frid), new Text(UDID));
				
			}else if(Action.equalsIgnoreCase("click.buy")&&DirectObject.equalsIgnoreCase("supermarketNative")){//特产代购 -- 立即购买
				output.collect(new Text("TShoppingBuyNow"+"\t"+OS+"\t"+CTID), new Text(UDID));		
				output.collect(new Text("TShoppingBuyNow"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));
				
			}else if(Action.equalsIgnoreCase("switch.Cart")&&DirectObject.equalsIgnoreCase("supermarketNative")){//特产代购 -- 进入水果篮
				output.collect(new Text("TShoppingBasket"+"\t"+OS+"\t"+CTID), new Text(UDID));			
				output.collect(new Text("TShoppingBasket"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));
				
			}else if(Action.equalsIgnoreCase("click.subsed")&&DirectObject.equalsIgnoreCase("supermarketNative")){//特产代购 -- 确认订单
				output.collect(new Text("TShoppingOrderConfirm"+"\t"+OS+"\t"+CTID), new Text(UDID));			
				output.collect(new Text("TShoppingOrderConfirm"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));
				
			}else if(Action.equalsIgnoreCase("switch.life") && DirectObject.equalsIgnoreCase("supermarket")){ //宅生活 - 切换宅生活
				output.collect(new Text("SwitchLife"+"\t"+OS+"\t"+CTID), new Text(UDID));			
				output.collect(new Text("SwitchLife"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));
				
			}else if(Action.equalsIgnoreCase("click.life") && DirectObject.equalsIgnoreCase("supermarket")){ //宅生活 - 点击网站-宅生活
				output.collect(new Text("ClickLife"+"\t"+OS+"\t"+CTID), new Text(UDID));			
				output.collect(new Text("ClickLife"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));
				
				String website=IndirectObject;
				output.collect(new Text("ClickLife_website"+"\t"+OS+"\t"+CTID+"\t"+website), new Text(UDID));	
				output.collect(new Text("ClickLife_website"+"\t"+OS+"\t"+CTID_whole+"\t"+website), new Text(UDID));
			}
			
			if(DirectObject.equalsIgnoreCase("supermarketNative")
					||(Action.equalsIgnoreCase("switch.Native")&&DirectObject.equalsIgnoreCase("supermarket"))){//特产代购总访问量
				output.collect(new Text("TShoppingTotal"+"\t"+OS+"\t"+CTID), new Text(UDID));			
				output.collect(new Text("TShoppingTotal"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));
			}else if(DirectObject.equalsIgnoreCase("supermarketfruit")
					||(Action.equalsIgnoreCase("switch.fruit")&&DirectObject.equalsIgnoreCase("supermarket"))
					||(Action.equalsIgnoreCase("shake")&&DirectObject.equalsIgnoreCase("Index")&&IndirectObject.equals("3"))){//水果代购总访问量
				output.collect(new Text("FShoppingTotal"+"\t"+OS+"\t"+CTID), new Text(UDID));			
				output.collect(new Text("FShoppingTotal"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));
			} else if (((Action.equalsIgnoreCase("switch.shopsale")	|| Action.equalsIgnoreCase("switch.list")|| 
					Action.equalsIgnoreCase("switch.bill") || Action.equalsIgnoreCase("switch.MinPriceTab") ||
					Action.equalsIgnoreCase("click.MinPriceSup")||Action.equalsIgnoreCase("show.activedetail")||
					Action.equalsIgnoreCase("click.activedetail")||Action.equalsIgnoreCase("click.poster"))
					&& DirectObject.equalsIgnoreCase("supermarket"))                                         //超市促销(包括宅生活)
					||(Action.equalsIgnoreCase("click.MinPriceSup")&&DirectObject.equalsIgnoreCase("supermarketPoster"))
					||(Action.equalsIgnoreCase("shake")&&DirectObject.equalsIgnoreCase("Index")&&IndirectObject.equals("2"))
					|| Action.equalsIgnoreCase("click.life")
					|| Action.equalsIgnoreCase("switch.life")) {
				output.collect(new Text("SupShoppingTotal"+"\t"+OS+"\t"+CTID), new Text(UDID));			
				output.collect(new Text("SupShoppingTotal"+"\t"+OS+"\t"+CTID_whole), new Text(UDID));
			}
		} catch (Exception e) {
			output.collect(new Text("err_log"), new Text(val.toString()));
			// e.printStackTrace();
		}		
	}
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		if(conn_order != null){
			try{
				conn_order.close();
				conn_order = null;
			}catch(Exception e){
				
			}
			conn_order = null;			
		}
		super.close();
	}
	public void ConnectToDb(){
		String driver = "com.mysql.jdbc.Driver";
		String url = "jdbc:mysql://192.168.1.9:3306/gcore";
		String user = "da_group";
		String password = "da_group_wcc";		
		try {
			Class.forName(driver);
			conn_order = DriverManager.getConnection(url, user, password);
			if(!conn_order.isClosed()){
				System.out.println("Succeeded connecting to the Database!");
			}			
		} catch (ClassNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public String queryCatId1(String CLID){
		String CatId1=null;
		ResultSet rs=null;
		PreparedStatement pstmt=null;
		try {
			String sql="select SQL_NO_CACHE distinct CatId1 from WCC_BYProduct_Category where CatId2=? or CatId3=?";
			pstmt = conn_order.prepareStatement(sql);
			pstmt.setString(1, CLID);
			pstmt.setString(2, CLID);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				CatId1 = rs.getString("CatId1");
				if(CatId1.length()!=0){
					break;
				}
			}
			rs.close();
			pstmt.close();
			conn_order.close();
			conn_order = null;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if(CatId1==null || CatId1.length()==0){
			CatId1="unknown";
		}
		return CatId1;
	}
}
