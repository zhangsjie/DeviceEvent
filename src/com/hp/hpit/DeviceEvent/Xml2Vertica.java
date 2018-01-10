package com.hp.hpit.DeviceEvent;

import java.io.IOException; 
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List; 
import java.util.Random;
//import java.util.regex.Matcher;
//import java.util.regex.Pattern;







import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem; 
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.BytesWritable; 
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;   
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner; 
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.hp.hpit.DeviceEvent.MyXmlHelper;
import com.hp.hpit.DeviceEvent.TextArrayWritable;
import com.vertica.hadoop.VerticaOutputFormat1;
import com.vertica.hadoop.VerticaRecord1;

public class Xml2Vertica extends Configured implements Tool { 

public static class xml2txt
extends Mapper<TextArrayWritable, BytesWritable, NullWritable, Text>
{
	private static final Log LOG = LogFactory.getLog("com.hp.deviceevent.xml2textmapper");
	MyXmlHelper xmlhelper = new MyXmlHelper();
	String tableNames = "";
	 
protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			String defaultTableNames = "TTF_Device_Incident_LZ.Fact_Stage_Common_Information_Model_Alert_Indication,TTF_Device_Incident_LZ.Fact_Stage_Device_Information,TTF_Device_Incident_LZ.Fact_Stage_Value_Pair";
			//updated at 2015/1/14
			//tableNames = context.getConfiguration().get("mapred.vertica.tablenames", defaultTableNames);
			tableNames = context.getConfiguration().get("mapred.vertica.tablenamewithschema", defaultTableNames);
}
	 
public void map(TextArrayWritable key, BytesWritable value, Context context ) throws IOException, InterruptedException
{
	 String zipFileName = "";
	 String xmlFileName = "";
	 Text[] tkey =(Text[])key.toArray();
	 try{
		 zipFileName = tkey[0].toString();
		 if(!tkey[1].toString().equals(""))
		 {
			 xmlFileName = tkey[1].toString().split("\\.")[0];
		 }
	 }
	 catch(Exception e)
	 {
		 LOG.info("not get zipfilename or xmlfilename, message:" + e.getMessage());
		 return;
	 }
	 if(value == null || value.getBytes().length < 4) return;
	 byte [] bc = new byte[value.getBytes().length-3];
	 if(value.getBytes().length>3
	 && value.getBytes()[0]==-17
	 && value.getBytes()[1]==-69
	 && value.getBytes()[2]==-65)
	 { 
	 	for(int i=0;i<value.getBytes().length-3;i++) bc[i]=value.getBytes()[i+3];
	 }
	 else bc= value.getBytes();
	 String content = new String(bc, "UTF-8" );
	 if(content.equals("")) return;
	 List<ArrayList<String>> output = null;
	 try {output = xmlhelper.ExtractXmlNodes(content, zipFileName, xmlFileName, tableNames);} catch (Exception e) 	
	 {
	   	 LOG.info("xml file name:" + xmlFileName.toString() +" zip file name:" + zipFileName.toString());
     }    
	 if (output!=null && output.size() > 0) 	 
	 {	 		
		 for (ArrayList<String> al : output)
		 {
			context.write(NullWritable.get(), new Text(xmlhelper.CombineArray2String(al)));
		 }
	 } 
}
}

public static class XMLMapper
extends Mapper<TextArrayWritable, BytesWritable, Text, TextArrayWritable>
{
	 private static String tableName = "";
	 private static String tableNames = "";
	 private static final Log LOG = LogFactory.getLog("com.hp.XMLMAPPER");
	 MyXmlHelper xmlhelper = new MyXmlHelper();
	 
protected void setup(Context context
     ) throws IOException, InterruptedException {
	super.setup(context); 
	String defaultTableNames = "TTF_Device_Incident_LZ.Fact_Stage_Common_Information_Model_Alert_Indication,TTF_Device_Incident_LZ.Fact_Stage_Device_Information,TTF_Device_Incident_LZ.Fact_Stage_Value_Pair";
	//updated at 2015/1/14
	//tableNames = context.getConfiguration().get("mapred.vertica.tablenames", defaultTableNames);
	tableNames = context.getConfiguration().get("mapred.vertica.tablenamewithschema", defaultTableNames);
}

public void map( TextArrayWritable key, BytesWritable value, Context context ) throws IOException, InterruptedException
{
	 String zipFileName = "";
	 String xmlFileName = "";
	 Text[] tkey =(Text[])key.toArray();
	 try{
		 zipFileName = tkey[0].toString();
		 if(!tkey[1].toString().equals(""))
		 {
			 xmlFileName = tkey[1].toString().split("\\.")[0];
		 }
	 }
	 catch(Exception e)
	 {
		 LOG.info("not get zipfilename or xmlfilename, message:" + e.getMessage());
		 return;
	 }
	 if(value == null || value.getBytes().length < 4) return;
	 byte [] bc = new byte[value.getBytes().length-3];
	 if(value.getBytes().length>3
	 && value.getBytes()[0]==-17
	 && value.getBytes()[1]==-69
	 && value.getBytes()[2]==-65)
	 { 
	 	for(int i=0;i<value.getBytes().length-3;i++) bc[i]=value.getBytes()[i+3];
	 }
	 else bc= value.getBytes();
	 String content = new String(bc, "UTF-8" );
	 if(content.equals("")) return;	
	 List<ArrayList<String>> output = null;
	 /* commented to reload OOS Properties tags
	 //get all stage table name list
	 String[] tableNameList = tableNames.split(",");
	 if(tableNameList.length < 3) 
	 {
	 	LOG.info("mapred.vertica.tablenames parameter is invalid!");
	 	return;
	 }
	 */
	 try
	 {
		 output = xmlhelper.ExtractXmlNodes(content, zipFileName, xmlFileName, tableNames);
		 //output = xmlhelper.ExtractXmlNodesForHistoryDataLoad(content, zipFileName, xmlFileName, tableNames);
	 }
	 catch (Exception e) 	
	 {
	   	 LOG.info("xml file name:" + xmlFileName.toString() +" zip file name:" + zipFileName.toString());
	 }
     if(output != null && output.size() > 0)
     {
         TextArrayWritable record = new TextArrayWritable();
  		 for (ArrayList<String> al : output)
  		 { 
  			 tableName = al.get(0);				
			 Text[] tt = new Text[al.size()-1];
			 for(int i=1;i<al.size();i++)
					 tt[i-1]= new Text(al.get(i));				
				record.set(tt);
				context.write(new Text(tableName), record);
  		 }
     }
 }
}

public static class VerticaReducer extends 
Reducer<Text,TextArrayWritable, Text, VerticaRecord1> { 
	  MyFileHelper fh = new MyFileHelper();
	  String rejectfilename ="";
	  
VerticaRecord1 record = null; 
HashMap<Text,VerticaRecord1> recordMap = new HashMap<Text,VerticaRecord1>();
boolean isnewapi = true;
//private static final Log LOG = LogFactory.getLog("VerticaReducer");
// Sets up the output record that will be populated by 
// the reducer and eventually written out. 
public void setup(Context context) throws IOException, 
    InterruptedException { 
    super.setup(context); 
    try {
    	isnewapi = context.getConfiguration().getBoolean("mapred.isnewapi", true);
  	    if(isnewapi)
  	    {    
  	    	String[] tables = context.getConfiguration().getStrings("mapred.vertica.tablenames");
  	    	//updated at 2015/1/14
  	    	String schemaname = context.getConfiguration().get("mapred.vertica.schemaname","TTF_Device_Incident_LZ");
  	    	for(String table:tables)
    		//recordMap.put(new Text(table.toUpperCase()), new VerticaRecord(context.getConfiguration(),new Text(table))); 
  	   	    recordMap.put(new Text(schemaname.toUpperCase() + "."+ table.toUpperCase()), new VerticaRecord1(context.getConfiguration(),new Text(schemaname + "." + table)));
  	    }
		else
			record = new VerticaRecord1(context.getConfiguration());

  	    } catch (Exception e) { throw new IOException(e);  } 
    
    try{
    	rejectfilename = "rejectedrows_" + context.getJobID()+"_" + context.getTaskAttemptID()+".txt";
    	fh.GetUniqueFile(context.getConfiguration(),rejectfilename);}catch(Exception ignore){ignore.getStackTrace();}    	
} 
 
// The reduce method. 
public void reduce(Text key, Iterable<TextArrayWritable> values, 
        Context context) throws IOException, InterruptedException { 	
    if(isnewapi)
    	record = recordMap.get(new Text(key.toString().toUpperCase()));
    if (record == null) { 
    	throw new IOException("No output record found"); 
    	} 
    boolean invalidlength = false;
    HashMap<String, Integer> nameMap = record.getColumnName();
    String tableName = record.getTableName();    
    for(TextArrayWritable value :values)
    {
    	String[] strs= value.toStrings();
    	String columnName = ""; 
    	for(int i =0;i<strs.length;i++)
    	{ 
    		try
    		{
    		invalidlength = record.set2(i, strs[i])?false:true;
    		}
    		catch(Exception e)
    		{
    			//updated at 2015/1/13
    			//LOG.info("Column index:"+ i +" column value: "+ strs[i]);
    			//LOG.info("Column name:" + record.getNames().get(i) + " Error column value:"+ strs[i]);
    			invalidlength = true;
    		}
    		if(invalidlength)
    		{
    	    	Iterator<String> it = nameMap.keySet().iterator();
    			while(it.hasNext())
    			{
    				Object keyString=it.next();
    				if(nameMap.get(keyString).equals(i))
    					columnName = keyString.toString();
    				}    			 
    			String info = tableName + "^" + columnName + "^invalidindex i:" + String.valueOf(i) + 
    					"^invalidlength:"+ strs[i].length() + "^Value:"+ strs[i]+ "^";
    			try
    			{
    				fh.writeObject(info);
    				fh.writeObject(fh.CombineArray2String(strs,"^","\n"));
    				}
    			catch(Exception ignore){}
    			}
    	}
    	context.write(key, record); 
    }
}
  
@SuppressWarnings("static-access")
protected  void cleanup(Context context
        ) throws IOException, InterruptedException {
	super.cleanup(context);    
	try{if(fh!=null) fh.close();
		fh.DeleteEmptyFile(context.getConfiguration(),rejectfilename);	
	}	catch(Exception e){e.getStackTrace();}
}
}

///use a Random generate even distribute 0---n
public static class MyPartitioner<K, V> extends Partitioner<K, V> {
private static Random rdmgenerater = new Random();
public int getPartition(K key, V value,int numReduceTasks) {
		return rdmgenerater.nextInt(numReduceTasks);
	  }
}

//split result to two files
public static class MyFilePartitioner<K, V> extends Partitioner<K, V> {
	 public int getPartition(K key, V value,int numReduceTasks) {
		 //TBD
		 return 1 % numReduceTasks;
		 /*
		 boolean istable1 = value.toString().split("\002")[0].equals("stage_personal_system_diagnostic.Fact_Stage_Run_Configuration")?true:false;
		 if(istable1) return 0;
		 else return 1 % numReduceTasks;
		 */
		  }
	}

public static void  main(String[] args) throws IOException {
		try {
			int res = ToolRunner.run(new Configuration(), new Xml2Vertica(), args);
			System.exit(res);			     	
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

public int run(String[] args) throws Exception {
		Configuration conf = getConf(); 
		customeProcessParameter(conf,args);
		boolean isdebug = conf.getBoolean("mapred.debug", false);
		//boolean isnewapi = conf.getBoolean("mapred.isnewapi", true);
		if(isdebug==false)
		{
			boolean jobresult = false;
			if(conf.get("mapred.vertica.tablenames")==null)
			{
				{System.out.println("mapred.vertica.tablenames should have value for new api such as schema.tablename,schema.tablename2"); return 1;}
			}
			else
				System.out.println(conf.get("mapred.vertica.tablenames"));
			Job job = executeOneJob(conf,args);
			job.waitForCompletion(true);
			processRejectRows(job,args);
			jobresult = job.isSuccessful();	
			return jobresult?0:1;
		}
		else  // xml 2 text
		{ 
			boolean jobresult = false;		
			Job job = executeOneJob(conf,args);
			job.waitForCompletion(true);
			processRejectRows(job,args);
			jobresult = job.isSuccessful();
			return jobresult?0:1;
		}		 
	}

private void customeProcessParameter(Configuration conf,String[] args)
{
	for(int i=0;i<args.length;i++)
	{ 
		if(args[i].equals("-D"))
			{
				String[] s = args[i+1].split("=");
				if(s[0].toLowerCase().equals("mapred.vertica.hostnames"))
					conf.setStrings(s[0],s[1].split(","));	
				else 
				conf.set(s[0],s[1]);
			}
	}
	int splitsize = conf.getInt("mapred.splitsize", 256);
	CombineXmlFileInputFormat.setSplitSize(splitsize);
 
}

private void processRejectRows(Job job,String[] args)
{
	Configuration conf = job.getConfiguration();
	   try{
		   //merge rejectedrows.txt
		   FileSystem srcFS = FileSystem.get(new Path(args[1]).toUri(), conf);
		   Path rejectedrows = new Path(args[1]+"/RejectedRows/");
		   String dstFile = args[1]+"/RejectedRows/Rejectedrows_"+job.getJobID()+".txt";
		   FileSystem dstFS= FileSystem.get(rejectedrows.toUri(),conf);
		   if(!dstFS.exists(rejectedrows) || !dstFS.getFileStatus(rejectedrows).isDir())
			   dstFS.mkdirs(rejectedrows);
		   MyFileHelper.copyMerge(srcFS, new Path(args[1]), dstFS,new Path(dstFile), false, conf,"rejectedrows_" + job.getJobID());
		   MyFileHelper.DeleteEmptyFile(conf, dstFile);
   }
   catch(Exception ignore){};	
}

private Job  executeOneJob(Configuration conf,String[] args)
{
	try
	{
	boolean truncate  = conf.getBoolean("mapred.vertica.truncate",true);
	boolean isdebug   = conf.getBoolean("mapred.debug", false);
	boolean ismaponly = conf.getBoolean("mapred.ismaponly", false);
	boolean isnewapi =  conf.getBoolean("mapred.isnewapi", true);
	boolean usepathfilter = conf.getBoolean("mapred.pathfilter",false); 
	
	//added at 2015/1/14 to get schema name and table name automatically
	String tableNames = conf.get("mapred.vertica.tablenames","Fact_Stage_Common_Information_Model_Alert_Indication,Fact_Stage_Device_Information,Fact_Stage_Value_Pair");
	String schemaName = conf.get("mapred.vertica.schemaname","TTF_Device_Incident_LZ");
    String[] singleTableName = tableNames.split(",");
    String tableNamesWithSchema = "";
    int num = singleTableName.length;
    if(num != 3)
    {
    	tableNamesWithSchema = schemaName + ".Fact_Stage_Common_Information_Model_Alert_Indication," + schemaName +
    			".Fact_Stage_Device_Information" + schemaName + ".Fact_Stage_Value_Pair";
    }
    else
    { 
    	for(int i = 0; i < singleTableName.length; i++)
    	{
    		tableNamesWithSchema += schemaName+"." + singleTableName[i];
    		if(i != singleTableName.length - 1)
    			tableNamesWithSchema += ",";
    	}
    }
    conf.set("mapred.vertica.tablenamewithschema", tableNamesWithSchema);    
	
	Job job = new Job(conf,this.getClass().getSimpleName());
	if(usepathfilter)
		FileInputFormat.setInputPathFilter(job,MyPathFilter.class);
	
	if(isdebug)//xml2text
	{ 
		job.setJarByClass(Xml2Vertica.class);
		job.setJarByClass(this.getClass());
		job.setMapperClass(xml2txt.class);
        job.setMapOutputKeyClass(NullWritable.class); 
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(CombineXmlFileInputFormat.class);
        job.setPartitionerClass(MyFilePartitioner.class);
        
        //if the output exists just drop
      		Path outPath = new Path(args[1]);
      		FileSystem dfs = FileSystem.get(outPath.toUri(), conf);
      		if (dfs.exists(outPath)) {
      			dfs.delete(outPath, true);
      		} 
      		
      		FileInputFormat.addInputPath(job, new Path(args[0]));
 	        FileOutputFormat.setOutputPath(job, new Path(args[1]));
 	        
      	job.setOutputFormatClass(MyTextOutputFormat.class);
        job.setNumReduceTasks(2);
		job.waitForCompletion(true);
	}
	else
	{  
		job.setJarByClass(Xml2Vertica.class);
		job.setInputFormatClass(CombineXmlFileInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
       
        if(ismaponly)
        {
	        job.setMapOutputValueClass(VerticaRecord1.class);
	        //job.setMapperClass(XML2VerticaRecordMapper.class);
	        job.setNumReduceTasks(0);
	        job.setSpeculativeExecution(true);
        }
        else
        {
	        job.setMapperClass(XMLMapper.class);
	        job.setMapOutputValueClass(TextArrayWritable.class);
	        job.setReducerClass(VerticaReducer.class);
	        job.setOutputKeyClass(Text.class); 
	        job.setOutputValueClass(VerticaRecord1.class); 
	        job.setPartitionerClass(MyPartitioner.class);
	    	job.setOutputFormatClass(VerticaOutputFormat1.class);
        }
        if(isnewapi)
        {        	
        	VerticaOutputFormat1.setOutputs(job, 
        		//conf.get("mapred.vertica.tablenames")
        		tableNamesWithSchema
        		, truncate);
        }
        else
        {
        	VerticaOutputFormat1.setOutput(job, 
        		conf.get("mapred.vertica.tablename")
        		, truncate);
        }
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
	} 
	
	 return job;
	}
	catch(Exception ex)
	{
		ex.getStackTrace();
		return null;
	}
}

}
