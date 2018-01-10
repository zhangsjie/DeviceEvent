package com.hp.hpit.DeviceEvent;

  
import java.io.IOException; 

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured; 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.FileSystem; 

public class MyPathFilter extends Configured implements PathFilter { 
	Configuration conf;
	FileSystem fs;
	public MyPathFilter(){} 
	@Override public boolean accept(Path path) 
	{ 
		try { 
			//TBD
			if(fs.getFileStatus(path).isDir())
				return true;
			else
				if(path.getName().toUpperCase().endsWith(".ZIP") 
						&& path.getName().toUpperCase().contains("_") ) 
				 return true;  
				else
					return false;
		} catch (IOException ignore) {
			return false;
			} 
		
	} 
	
	@Override 
	public void setConf(Configuration conf) 
	{ 
		this.conf=conf;
		 if (conf != null) {
	            try {
	                fs = FileSystem.get(conf);
	            } catch (Exception e) {
	                e.printStackTrace();
	            }
	        }
	} 
	
}