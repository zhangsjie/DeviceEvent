package com.hp.hpit.DeviceEvent;

import java.io.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;

public  class MyFileHelper {

	private static final Log LOG = LogFactory.getLog("FileHelper");
	
	protected DataOutputStream out;
	
	public MyFileHelper(DataOutputStream out)
	{
		this.out=out;
	}
	
	public MyFileHelper(){}
	
	public synchronized static void WriteLine(String filename,String _contents)
	{		
		//create file if not exists
		File _file = new File(filename);
		
		if(!_file.exists())
			try {
				_file.createNewFile();
			} catch (IOException e) {
				LOG.info(e.getMessage());
			}
		//write contents
		FileOutputStream fos = null;
		try {
			  fos= new FileOutputStream(filename,true);
			   try {
				fos.write(_contents.getBytes());
				fos.write("\r\n".getBytes());
			} catch (IOException e) {
				LOG.info(e.getMessage());
			}

		} catch (FileNotFoundException e) {
			LOG.info(e.getMessage());
		}
		finally
		{
			try {
				if(null!=fos)
					fos.close();
			} catch (IOException ignore) {
			}
		}
	}

	public synchronized void writeObject(Object o) throws IOException {
	      if (o instanceof Text) {
	        Text to = (Text) o;
	        out.write(to.getBytes(), 0, to.getLength());
	      } else {
	        out.write(o.toString().getBytes("UTF-8"));
	      }
	    }
	
	public synchronized void GetUniqueFile(Configuration conf,String filename) throws IOException
	{
		  Path file = new Path(conf.get("mapred.output.dir"),filename);
		  FileSystem fs = file.getFileSystem(conf);
		  FSDataOutputStream fileOut = fs.create(file, true);
		  this.out=fileOut;
	}
	
	public static void DeleteEmptyFile(Configuration conf,String filename) throws IOException
	{
		 Path file = new Path(conf.get("mapred.output.dir"),filename);
		 FileSystem fs = file.getFileSystem(conf);
		 if(fs.getFileStatus(file).getLen()==0
				 || fs.getFileStatus(file).getBlockSize()==0
				 ) 
			 try{
				 fs.delete(file,false);
			 }catch(Exception ex){ ex.printStackTrace();}
	}
	
	/** Copy all files in a directory to one output file (merge). */
    public static boolean copyMerge(FileSystem srcFS, Path srcDir, 
	                               FileSystem dstFS, Path dstFile, 
	                               boolean deleteSource,
	                               Configuration conf,String prefix) throws IOException {

	    if (!srcFS.getFileStatus(srcDir).isDir())
	      return false;
	   if(prefix==null) return false;
	   
	    OutputStream out = dstFS.create(dstFile);
	    
	    try {
	      FileStatus contents[] = srcFS.listStatus(srcDir);
	      for (int i = 0; i < contents.length; i++) {
	        if (!contents[i].isDir()) {
	          if(!contents[i].getPath().getName().startsWith(prefix)) continue;
	          InputStream in = srcFS.open(contents[i].getPath());
	          try {
	            IOUtils.copyBytes(in, out, conf, false);
	          } finally {
	            in.close();
	            srcFS.delete(contents[i].getPath(),false);
	          } 
	        }
	      }
	    } finally {
	      out.close();
	    }
	     
	    if (deleteSource) {
	      return srcFS.delete(srcDir, true);
	    } else {
	      return true;
	    }
	  }  
	  
	public synchronized void close()
	{
		if(out!=null)
			try {
				out.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
	}
	
	public  String CombineArray2String(String[] al,String coldelimiter,String rowTerminater)
	{
		//\002 as the column delimiter \001 as the row delimiter
		StringBuilder sb = new StringBuilder();
		for(int i=0;i<al.length-1;i++)
		{
			sb.append(al[i]);
			sb.append(coldelimiter) ;
		}
		sb.append(al[al.length-1]);
		sb.append(rowTerminater);
		return sb.toString();
	}
}
