package com.wochacha.da.transform.lyg;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import com.wochacha.da.model.JobManager;
import com.wochacha.da.util.MsgSender;

public class LYGReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>
{
    public String            job_date = "";

    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
    {
        String Date = job_date;
        String OS = "";
        String Ver = "";
        String CTID = "";
        String funType = "";
        String UDID = "";
        String productType = "";
        String objectId = "";
        int UV = 0;
        int PV = 0;

        HashSet<String> udidSet = new HashSet<String>();
        
        String[] keySplitArr = key.toString().split("\t");
        funType = keySplitArr[0];
        CTID = keySplitArr[1];
        OS = keySplitArr[2];
        Ver = keySplitArr[3];
        productType = keySplitArr[4];
        objectId = keySplitArr.length==6 ? keySplitArr[5] : "";
        
        while (values.hasNext())
        {
            String[] value = values.next().toString().split("\t");
            UDID = value[0];
            udidSet.add(UDID);
            PV++;
        }
        UV = udidSet.size();
        
        
        if ( keySplitArr.length == 5 )//统计每一个栏目
        {
            String[] volConName = new String[] { "Date", "ProductType", "module", "CTID", "FunType", "OS", "Ver" };
            String[] volConValue = new String[] { Date, productType, "freebuy",  CTID, funType, OS, Ver };
            String[] volNameInsert = new String[] { "Date", "ProductType", "module", "CTID", "FunType", "OS", "Ver", "PV", "UV" };
            String[] volValueInsert = new String[] { Date, productType, "freebuy", CTID, funType, OS, Ver, PV + "", UV + "" };
            String[] VName = new String[] { "PV", "UV" };
            String[] VValue = new String[] { PV + "", UV + "" };
            
            try
            {
                JobManager.ResultToDB(output, "DA_FreebuyItem_Analysis", volConName, volConValue, volNameInsert, volValueInsert, VName, VValue,"");
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
                MsgSender.sendMessage(new String[] { "err", "Insert error\t" + job_date + "\tLYGReducer" + "\t" + e.getMessage() });
            }
        }
        
        
        if( keySplitArr.length == 6 )//统计详情
        {
            String[] volConName = new String[] { "Date", "ProductType", "module", "CTID", "FunType", "OS", "Ver", "ObjectId" };
            String[] volConValue = new String[] { Date, productType, "freebuy",  CTID, funType, OS, Ver, objectId };
            String[] volNameInsert = new String[] { "Date", "ProductType", "module", "CTID", "FunType", "ObjectId", "OS", "Ver", "PV", "UV" };
            String[] volValueInsert = new String[] { Date, productType, "freebuy", CTID, funType, objectId, OS, Ver, PV + "", UV + "" };
            String[] VName = new String[] { "PV", "UV" };
            String[] VValue = new String[] { PV + "", UV + "" };
            
            try
            {
                JobManager.ResultToDB(output, "DA_FreebuyDetail_Analysis", volConName, volConValue, volNameInsert, volValueInsert, VName, VValue,"");
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
                MsgSender.sendMessage(new String[] { "err", "Insert error\t" + job_date + "\tLYGReducer" + "\t" + e.getMessage() });
            }
        }

    }

    @Override
    public void configure(JobConf conf)
    {
        super.configure(conf);
        job_date = conf.get("job_date");
    }

}
