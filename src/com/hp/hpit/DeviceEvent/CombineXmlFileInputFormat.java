package com.hp.hpit.DeviceEvent;

import java.io.IOException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext; 

public class CombineXmlFileInputFormat extends MyCombineFileInputFormat<TextArrayWritable, BytesWritable> {
	 /** See the comments on the setLenient() method */
    private static boolean isLenient = false;
    private static int splitsize=256;
    
	public CombineXmlFileInputFormat(){
	    super();
	    setMaxSplitSize(1024*1024*splitsize); // 64*4 MB, default block size on mapr hadoop
	  } 
	  
	  /**
	  * ZIP files are not splitable
	  */
	     @Override
	      protected boolean isSplitable( JobContext context, Path filename )
	      {
	          return false;
	      }
	      /**
	      * Create the ZipFileRecordReader to parse the file
	      */
	     @Override
	      public RecordReader<TextArrayWritable, BytesWritable> createRecordReader( InputSplit split, TaskAttemptContext context )
	              throws IOException
	          {
	        	    return new CombineFileRecordReader<TextArrayWritable, BytesWritable>((CombineFileSplit)split, context, CombineXmlFileRecordReader.class);
	          }
	          
	          /**
	      *
	      * @param lenient
	      */
	          public static void setLenient( boolean lenient )
	          {
	              isLenient = lenient;
	          }
	          
	          public static boolean getLenient()
	          {
	              return isLenient;
	          }
	          
	          public static void setSplitSize(int _splitesize)
	          {
	        	  splitsize=_splitesize;
	          }

			 
}
