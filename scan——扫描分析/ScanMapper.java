package com.wochacha.da.transform.scan;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.wochacha.da.model.Record;

public class ScanMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text val, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {	
		String[] arr = val.toString().split("\t");
		String path = reporter.getInputSplit().toString();
		if(arr.length >= 8 ){
			String scanType = arr[0];
			String UDID= arr[2];
			String province_city = arr[3];
			String Channel = arr[4];
			String OS = arr[5];
			String Ver = arr[6];
			String ProductType = arr[7]; 
//			String operator = arr[8]; 
			String operator_all="all";
			output.collect(new Text("User_User_analysis"+"\t"+Channel+"\t"+OS+"\t"+Ver+"\t"+ProductType), new Text(UDID)); 
			
//			output.collect(new Text("UsrRegion_analysis"+"\t"+province_city+"\t"+Channel+"\t"+OS+"\t"+Ver+"\t"+ProductType+"\t"+operator), new Text(UDID)); 
			output.collect(new Text("UsrRegion_analysis"+"\t"+province_city+"\t"+Channel+"\t"+OS+"\t"+Ver+"\t"+ProductType+"\t"+operator_all), new Text(UDID)); 
			try {		
				if (path.indexOf("scanBarcodeFlag") >= 0) { 		       //条形码扫描
					output.collect(new Text(arr[0]+"\t"+Channel+"\t"+OS+"\t"+Ver+"\t"+ProductType), new Text(UDID));     
					
				}else if (path.indexOf("scan2DCode_") >= 0) { 		      //二维码扫描
					output.collect(new Text("scan2DCode"+"\t"+Channel+"\t"+OS+"\t"+Ver+"\t"+ProductType), new Text(UDID)); 
					
					if (path.indexOf("scan2DCode_QRCodeFlag") >= 0) {    			  	//QR码扫描
						output.collect(new Text(arr[0]+"\t"+Channel+"\t"+OS+"\t"+Ver+"\t"+ProductType), new Text(UDID));
						
					} else if (path.indexOf("scan2DCode_DMCodeFlag") >= 0) {         	//DM码扫描
						output.collect(new Text(arr[0]+"\t"+Channel+"\t"+OS+"\t"+Ver+"\t"+ProductType), new Text(UDID)); 
						
					} /*else if (path.indexOf("scan2DCode_HCodeFlag") >= 0) {         	//H码扫描
						output.collect(new Text(arr[0]+"\t"+Channel+"\t"+OS+"\t"+Ver+"\t"+ProductType), new Text(UDID)); 
						
					} */else if (path.indexOf("scan2DCode_WEPCCodeFlag") >= 0) {         	//WEPC码扫描
						output.collect(new Text(arr[0]+"\t"+Channel+"\t"+OS+"\t"+Ver+"\t"+ProductType), new Text(UDID)); 
					} else if (path.indexOf("scan2DCode_HxCodeFlag") >= 0) {         	//汉信码扫描
						output.collect(new Text(arr[0]+"\t"+Channel+"\t"+OS+"\t"+Ver+"\t"+ProductType), new Text(UDID)); 
					}
					
				}else if (path.indexOf("scanExpressFlag") >= 0) { 		   //快递单码扫描
					output.collect(new Text(arr[0]+"\t"+Channel+"\t"+OS+"\t"+Ver+"\t"+ProductType), new Text(UDID)); 
					
				}else if (path.indexOf("scanDrugFlag") >= 0) { 		       //药监码扫描
					output.collect(new Text(arr[0]+"\t"+Channel+"\t"+OS+"\t"+Ver+"\t"+ProductType), new Text(UDID)); 
				}
			} catch (Exception e) {
				output.collect(new Text("err_log"), new Text(val.toString()));
				// e.printStackTrace();
			}
		}
		
	}
}
