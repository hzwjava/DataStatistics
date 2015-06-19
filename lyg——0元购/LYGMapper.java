package com.wochacha.da.transform.lyg;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class LYGMapper implements Mapper<LongWritable, Text, Text, Text>
{

    //表示进入零元购活动详情后的状态: 即将开始,抽奖进行中,待开奖,待秒杀,秒杀中,抢购中,结束
    String[] activityStatus = {"Begining",  "Running", "WaitOpen", "WaitKill", "Killing", "Buying", "End"};
    //表示进入零元购活动详情的入口: 零元购主页,我的活动主页,首页
    String[] activityEntrances = {"Freebuy", "MyActivity", "Home"};
    //表示进入商品详情的入口: 零元购立即秒杀，零元购抢购，其他
    String[] commodityEntrances = {"Kill", "Buy", "Other"};
    //表示我参与活动中奖状态: 未中奖,福利将,大奖,活动进行中
    String[] prizeStatus = {"No", "Welfare", "Prize", "Running"};
    
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
    {
        String[] arr = value.toString().split("\t");
        if( arr.length !=26 ) return;
        String udid = arr[2];
        String os = arr[6];
        String ver = arr[7];
        String ctid = arr[9];
        String action = arr[16];
        String directObject = arr[17];
        String indirectObject = arr[18];
        String result = arr[19];
        String message = arr[20];
        String productType = arr[22];
        
        String objectId = "";
        
        String CTID_whole = "4275";
        action = action.trim();
        indirectObject = indirectObject.trim();
        String funtype = "";
        
        if (action.equalsIgnoreCase("click.freebuy") && directObject.equalsIgnoreCase("freepurchase"))
        {
            funtype = "ClickFreebuy";//进入零元购
            output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            //equalsIgnoreCase无视大小，长度相等，字符相等
            if(indirectObject.equalsIgnoreCase("1")){// 从首页进入
				funtype="FreebuyIndex";
				output.collect(new Text(funtype + "\t" + ctid + "\t" + os + "\t"
						+ ver + "\t" + productType), new Text(udid));
				output.collect(new Text(funtype + "\t" + CTID_whole + "\t" + os
						+ "\t" + ver + "\t" + productType), new Text(udid));
			}else if(indirectObject.equalsIgnoreCase("2")){// 从消息中心进入
				funtype="FreebuyMesCen";
				output.collect(new Text(funtype + "\t" + ctid + "\t" + os + "\t"
						+ ver + "\t" + productType), new Text(udid));
				output.collect(new Text(funtype + "\t" + CTID_whole + "\t" + os
						+ "\t" + ver + "\t" + productType), new Text(udid));
			}else{// 老版本:不区分进入方式
				funtype="FreebuyOld";
				output.collect(new Text(funtype + "\t" + ctid + "\t" + os + "\t"
						+ ver + "\t" + productType), new Text(udid));
				output.collect(new Text(funtype + "\t" + CTID_whole + "\t" + os
						+ "\t" + ver + "\t" + productType), new Text(udid));
			}
        
        
        }else if (action.equalsIgnoreCase("switch.onlinesoon") && directObject.equalsIgnoreCase("freepurchase")){
            funtype = "SwitchOnlineSoon";//即将上线Tab
            output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            
        }else if (action.equalsIgnoreCase("switch.beforeactivity") && directObject.equalsIgnoreCase("freepurchase")){
            funtype = "SwitchBeforeActivity";//往期活动Tab
            output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid));
            output.collect( new Text("Total" + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            
        }else if (action.equalsIgnoreCase("switch.online") && directObject.equalsIgnoreCase("freepurchase")){
            funtype = "SwitchOnline";//正在进行Tab
            output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            
        }else if ( "access.activitydetail".equalsIgnoreCase(action) && "freepurchase".equalsIgnoreCase(directObject) ){
            if( Util.regMatch(result, "[0-6]") && Util.regMatch(message, "[1-3]") ){
                //即将开始活动页面  0元抽奖进行中页面   待开奖页面   待秒杀页面   秒杀中页面   抢购中页面   已结束页面
                funtype  = "Activity_" + this.activityStatus[Integer.parseInt(result)];
                objectId = indirectObject;
                output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
                output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
                output.collect( new Text("Total" + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
                output.collect( new Text("Total" + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
                //活动详情
                output.collect( new Text("ActivityDetail" + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType + "\t" + objectId), new Text(udid) );
                output.collect( new Text("ActivityDetail" + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType + "\t" + objectId), new Text(udid) );
            }
        }else if (action.equalsIgnoreCase("access.myactivity") && directObject.equalsIgnoreCase("freepurchase")){
            funtype = "AccessMyActivity";//我的页面
            output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            
        }else if (action.equalsIgnoreCase("click.myactivity") && directObject.equalsIgnoreCase("freepurchase")){
            if(Util.regMatch(result, "[0-3]") ){
                funtype = "ClickMyActivity";//我的活动页面
                output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
                output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
                output.collect( new Text("Total" + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
                output.collect( new Text("Total" + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            }
        }else if (action.equalsIgnoreCase("click.raffle") && directObject.equalsIgnoreCase("freepurchase")){
            funtype = "ClickRaffle";//点击“立即抽奖”
            output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            objectId = indirectObject;
            output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType + "\t" + objectId), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType + "\t" + objectId), new Text(udid) );       
        }else if (action.equalsIgnoreCase("Access.Commodity") && directObject.equalsIgnoreCase("ZXMerchant")){
            if(Util.regMatch(result, "[1-3]") )
            {
                funtype = "Commodity_" + this.commodityEntrances[Integer.parseInt(result)-1];////点击“立即秒杀”或“立即抢购”      
            }
            objectId = indirectObject;
            output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType + "\t" + objectId), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType + "\t" + objectId), new Text(udid) );  
            
        }else if (action.equalsIgnoreCase("Share.Sina") && directObject.equalsIgnoreCase("freepurchasedetail")){
            funtype = "RightTopShare";
            objectId = indirectObject;
            output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType + "\t" + objectId), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType + "\t" + objectId), new Text(udid) ); 
            
        }else if (action.equalsIgnoreCase("Share.QQ") && directObject.equalsIgnoreCase("freepurchasedetail")){
            funtype = "RightTopShare";
            objectId = indirectObject;
            output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType + "\t" + objectId), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType + "\t" + objectId), new Text(udid) );       
            
        }else if (action.equalsIgnoreCase("Share.Vchat") && directObject.equalsIgnoreCase("freepurchasedetail")){
            funtype = "RightTopShare";
            objectId = indirectObject;
            output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType + "\t" + objectId), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType + "\t" + objectId), new Text(udid) );                   

        }else if (action.equalsIgnoreCase("Share.Vfriend") && directObject.equalsIgnoreCase("freepurchasedetail")){
            funtype = "RightTopShare";
            objectId = indirectObject;
            output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType + "\t" + objectId), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType + "\t" + objectId), new Text(udid) );       
            
        }else if (action.equalsIgnoreCase("Share.Sms") && directObject.equalsIgnoreCase("freepurchasedetail")){
            funtype = "RightTopShare";
            objectId = indirectObject;
            output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType + "\t" + objectId), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType + "\t" + objectId), new Text(udid) );       
            
        }else if (action.equalsIgnoreCase("Share.Mail") && directObject.equalsIgnoreCase("freepurchasedetail")){
            funtype = "RightTopShare";
            objectId = indirectObject;
            output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType + "\t" + objectId), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType + "\t" + objectId), new Text(udid) );       
            
        }else if (action.equalsIgnoreCase("Share.Sina") && directObject.equalsIgnoreCase("freepurchasepop")){
            funtype = "PopShare";
            objectId = indirectObject;
            output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType + "\t" + objectId), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType + "\t" + objectId), new Text(udid) );       
            
        }else if (action.equalsIgnoreCase("Share.QQ") && directObject.equalsIgnoreCase("freepurchasepop")){
            funtype = "PopShare";
            objectId = indirectObject;
            output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType + "\t" + objectId), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType + "\t" + objectId), new Text(udid) );       
            
        }else if (action.equalsIgnoreCase("Share.Vchat") && directObject.equalsIgnoreCase("freepurchasepop")){
            funtype = "PopShare";
            objectId = indirectObject;
            output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType + "\t" + objectId), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType + "\t" + objectId), new Text(udid) );       
            
        }else if (action.equalsIgnoreCase("Share.Vfriend") && directObject.equalsIgnoreCase("freepurchasepop")){
            funtype = "PopShare";
            objectId = indirectObject;
            output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType + "\t" + objectId), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType + "\t" + objectId), new Text(udid) );       
            
        }else if (action.equalsIgnoreCase("Share.Sms") && directObject.equalsIgnoreCase("freepurchasepop")){
            funtype = "PopShare";
            objectId = indirectObject;
            output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType + "\t" + objectId), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType + "\t" + objectId), new Text(udid) );       
            
        }else if (action.equalsIgnoreCase("Share.Mail") && directObject.equalsIgnoreCase("freepurchasepop")){
            funtype = "PopShare";
            objectId = indirectObject;
            output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType + "\t" + objectId), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType + "\t" + objectId), new Text(udid) );       
            
        }else if (action.equalsIgnoreCase("Share.Sina") && directObject.equalsIgnoreCase("freepurchaseactivity")){
            funtype = "MyAcitvityShare";
            objectId = indirectObject;
            output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType + "\t" + objectId), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType + "\t" + objectId), new Text(udid) );       
            
        }else if (action.equalsIgnoreCase("Share.QQ") && directObject.equalsIgnoreCase("freepurchaseactivity")){
            funtype = "MyAcitvityShare";
            objectId = indirectObject;
            output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType + "\t" + objectId), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType + "\t" + objectId), new Text(udid) );       
            
        }else if (action.equalsIgnoreCase("Share.Vchat") && directObject.equalsIgnoreCase("freepurchaseactivity")){
            funtype = "MyAcitvityShare";
            objectId = indirectObject;
            output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType + "\t" + objectId), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType + "\t" + objectId), new Text(udid) );       
            
        }else if (action.equalsIgnoreCase("Share.Vfriend") && directObject.equalsIgnoreCase("freepurchaseactivity")){
            funtype = "MyAcitvityShare";
            objectId = indirectObject;
            output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType + "\t" + objectId), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType + "\t" + objectId), new Text(udid) );       
            
        }else if (action.equalsIgnoreCase("Share.Sms") && directObject.equalsIgnoreCase("freepurchaseactivity")){
            funtype = "MyAcitvityShare";
            objectId = indirectObject;
            output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType + "\t" + objectId), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType + "\t" + objectId), new Text(udid) );       
            
        }else if (action.equalsIgnoreCase("Share.Mail") && directObject.equalsIgnoreCase("freepurchaseactivity")){
            funtype = "MyAcitvityShare";
            objectId = indirectObject;
            output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType + "\t" + objectId), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType + "\t" + objectId), new Text(udid) );       
            
        }else if (action.equalsIgnoreCase("switch.welfare") && directObject.equalsIgnoreCase("freepurchase"))
        {
            funtype = "SwitchWelfare";//红包福利点击(tab)
            output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
  
        }else if (action.equalsIgnoreCase("click.luckychance") && directObject.equalsIgnoreCase("freepurchase")){
            funtype = "ClickLuckyChance";//幸运机会
            output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
     
        }else if (action.equalsIgnoreCase("click.luckyopp") && directObject.equalsIgnoreCase("freepurchase")){
            funtype = "ClickLuckyOpp";//添加幸运机会
            output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
    
        }else if (action.equalsIgnoreCase("click.lucyoppyes") && directObject.equalsIgnoreCase("freepurchase")){
            funtype = "ClickLuckyOppYes";//成功添加幸运机会
            output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
    
        }else if (action.equalsIgnoreCase("click.activitypic") && directObject.equalsIgnoreCase("freepurchasepop")){
            funtype = "ClickActivityPic";//点击活动图片跳转外部链接
            objectId = indirectObject;
            output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text("Total" + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType), new Text(udid) );
            output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType + "\t" + objectId), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType + "\t" + objectId), new Text(udid) );       
            
        }else if (action.equalsIgnoreCase("access.bannerdetail") && directObject.equalsIgnoreCase("banner")){
        	 funtype = "Banner";//点击活动图片跳转外部链接
             objectId = indirectObject;
        	output.collect( new Text(funtype + "\t" + ctid + "\t" + os + "\t" + ver + "\t" + productType + "\t" + objectId), new Text(udid) );
            output.collect( new Text(funtype + "\t" + CTID_whole + "\t" + os + "\t" + ver + "\t" + productType + "\t" + objectId), new Text(udid) );
            
        }
          
    }


    @Override
    public void configure(JobConf arg0)
    {
        // TODO Auto-generated method stub
    }

    @Override
    public void close() throws IOException
    {
        // TODO Auto-generated method stub
    }

}
