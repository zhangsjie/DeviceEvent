package com.hp.hpit.DeviceEvent;

//import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException; 
import java.io.InputStream;
//import java.util.Date;
//import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory; 
import org.apache.hadoop.io.compress.bzip2.CBZip2InputStream;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
//import org.vafer.jdeb.shaded.compress.compress.archivers.ArchiveEntry;
import org.vafer.jdeb.shaded.compress.compress.archivers.ArchiveInputStream;
import org.vafer.jdeb.shaded.compress.compress.archivers.tar.TarArchiveInputStream;
import org.vafer.jdeb.shaded.compress.compress.archivers.zip.ZipArchiveInputStream;
//import org.vafer.jdeb.shaded.compress.compress.utils.IOUtils;


 
public class CombineXmlFileRecordReader
    extends RecordReader<TextArrayWritable, BytesWritable>
{
	private static final Log LOG = LogFactory.getLog(CombineXmlFileRecordReader.class);
	
    /** InputStream used to read the ZIP file from the FileSystem */
    private FSDataInputStream fsin;
    
    private ArchiveInputStream ais;	
 
    /** ZIP file parser/decompresser */
    private ZipInputStream zip;
    
    /** Uncompressed file name */
    private TextArrayWritable currentKey;
    
    /** Uncompressed file contents */
    private BytesWritable currentValue;
    
    private Text fileName;

    /** Used to indicate progress */
    private boolean isFinished = true;

	private CombineFileSplit combineFileSplit;

	private Integer currentIndex;
	
    @Override
    public void initialize( InputSplit inputSplit, TaskAttemptContext taskAttemptContext )
        throws IOException, InterruptedException
    {
    	this.combineFileSplit = (CombineFileSplit) inputSplit;
        Configuration conf = taskAttemptContext.getConfiguration();
 
        FileSplit fileSplit = new FileSplit(
        		combineFileSplit.getPath(currentIndex),
        		combineFileSplit.getOffset(currentIndex),
        		combineFileSplit.getLength(currentIndex),
        		combineFileSplit.getLocations());
        Path path = fileSplit.getPath();
        FileSystem fs = path.getFileSystem(conf);        
        
        fileName =new Text( path.getName());
         
        fsin = fs.open( path );
        zip = new ZipInputStream( fsin );
        ais = DetectCodec(fileSplit,conf);
    }
 
    public CombineXmlFileRecordReader(CombineFileSplit combineFileSplit, TaskAttemptContext context, Integer index)
    	   throws IOException {
    		super();
    		this.combineFileSplit = combineFileSplit;
    		this.currentIndex = index;
     }
  
    public CombineXmlFileRecordReader()
     	   throws IOException {
      }
    
    private  String GetFileName(String filename)
    {
 	   String _filename ="";
 	   if (filename!=null)
 	   {
 		   if(filename.lastIndexOf("/")>0 && filename.lastIndexOf("/")+1<filename.length())
 		     _filename = filename.substring(filename.lastIndexOf("/")+1);
 		   else if (filename.lastIndexOf("\\")>0 && filename.lastIndexOf("\\")+1<filename.length())
 			 _filename=filename.substring(filename.lastIndexOf("\\")+1);
 		   else
 			   _filename=filename;
 	   }
 	   return _filename;
    }
  
    /**
* This is where the magic happens, each ZipEntry is decompressed and
* readied for the Mapper. The contents of each file is held *in memory*
* in a BytesWritable object.
*
* If the ZipFileInputFormat has been set to Lenient (not the default),
* certain exceptions will be gracefully ignored to prevent a larger job
* from failing.
     * @throws IOException 
*/
	@Override
    public boolean nextKeyValue() throws IOException 
    { 		
		ZipEntry entry = null;		
        try
        {
            entry = zip.getNextEntry();            
        }
        catch(EOFException e)
        {
        	if ( CombineXmlFileInputFormat.getLenient() == false )
                LOG.info("fileName:"+fileName+"  error info:"+e.getMessage());
        }
        catch ( IOException e )
        {
            if ( CombineXmlFileInputFormat.getLenient() == false )
                LOG.info("fileName:"+fileName+"  error info:"+e.getMessage());
        } 
        catch(Exception e)
        {
        	if ( CombineXmlFileInputFormat.getLenient() == false )
                LOG.info("fileName:"+fileName+"  error info:"+e.getMessage());
        }
        // Sanity check
        if ( entry == null)
        {
            isFinished = true;
            return false;
        }
        
        // Filename 
        currentKey = new TextArrayWritable();
        Text[] tkey = new Text[2];
        //0 is zip name 1 is xml file name 
        tkey[0] = fileName;
        tkey[1] =  new Text(GetFileName(entry.getName()));
        currentKey.set(tkey);
        
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte[] temp = new byte[8192];
        while ( true )
        {
            int bytesRead = 0;
            try
            {
                bytesRead = zip.read( temp, 0, 8192 );
            }
            catch (Exception e )
            {
            	if(CombineXmlFileInputFormat.getLenient() == false )
                    LOG.info("fileName:"+fileName+"  error info:"+e.getMessage());
            	
            	currentValue = new BytesWritable( "Parse zip Exception occur".getBytes());
            	try{ zip.closeEntry();} catch(Exception ignore){}
            	return true;
            }
            if ( bytesRead > 0 )
                bos.write( temp, 0, bytesRead );
            else
                break;
        }
        zip.closeEntry();
        
        // Uncompressed contents
        currentValue = new BytesWritable( bos.toByteArray() );
        return true;
    }

    /**
* Rather than calculating progress, we just keep it simple
*/
    @Override
    public float getProgress()
        throws IOException, InterruptedException
    {
        return isFinished ? 1 : 0;
    }

    /**
* Returns the current key (name of the zipped file)
*/
    @Override
    public TextArrayWritable getCurrentKey()
        throws IOException, InterruptedException
    {
        return currentKey;
    }	

    /**
* Returns the current value (contents of the zipped file)
*/
    @Override
    public BytesWritable getCurrentValue()
        throws IOException, InterruptedException
    {
        return currentValue;
    }

    /**
* Close quietly, ignoring any exceptions
*/
    @Override
    public void close()
        throws IOException
    {
        try { fsin.close(); } catch ( Exception ignore ) { ignore.getStackTrace();}
        try{ ais.close();} catch(Exception ignore){ignore.getStackTrace();};
    }
    
    private ArchiveInputStream DetectCodec(FileSplit filesplit,Configuration conf) throws IOException{
    	InputStream in = null;
        final Path path = filesplit.getPath();
 
        ArchiveInputStream ais;
	        if (path.toString().toLowerCase().endsWith(".zip"))
	        {
	        	  ais = new ZipArchiveInputStream(fsin);
	        }
	        else 
	        	{
		        	if(path.toString().endsWith(".bz2") || path.toString().endsWith(".bz"))
		            {
		            	// For bzip2 files use CBZip2InputStream to read and supply the upper input stream.
		                 in = new CBZip2InputStream(fsin); 
		            }
		            else if (path.toString().endsWith(".gz"))
		            {
		            	CompressionCodecFactory compressionCodecs =  new CompressionCodecFactory(conf);
		            	final CompressionCodec codec = compressionCodecs.getCodec(path);
		            	 if (codec != null) { 
		            	        in = codec.createInputStream(fsin); 
		            	    }
		            }
		            else {
		            	 throw new IllegalStateException("Unsupported compression format !");
		            }
			        ais= new TarArchiveInputStream(in);
	        	}
	        if(LOG.isDebugEnabled()){LOG.info("zip file name is : "+ path.toString());}
	        
		  return ais;
	  }
}