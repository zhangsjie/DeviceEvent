package com.hp.hpit.DeviceEvent;

//import java.io.IOException;
import java.io.StringReader; 
import java.text.SimpleDateFormat;
//import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;





//import java.util.regex.Matcher;
//import java.util.regex.Pattern;
import javax.xml.namespace.NamespaceContext;
import javax.xml.parsers.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.w3c.dom.*;
import org.xml.sax.InputSource;

import javax.xml.xpath.*;

final class XHTMLNamespaceContext implements NamespaceContext {  
	private String uri = "";
    public XHTMLNamespaceContext(String string) {
    	this.uri = string;
	}

	@Override  
    public String getNamespaceURI(String prefix) {  
        //return "http://www.hp.com/schemas/isee/5.00/event";
    	return uri;
    }       
    
    @Override  
    public String getPrefix(String namespaceURI) {  
        throw new UnsupportedOperationException();  
    }  
  
    @SuppressWarnings("rawtypes")
	@Override  
    public Iterator getPrefixes(String namespaceURI) {  
        throw new UnsupportedOperationException();  
    }  
  
}  
public  class MyXmlHelper {

private static final Log LOG = LogFactory.getLog("com.hp.xmlhelper");

private static final int serviceIncidentColumnCount = 13;

private static final int alertIndicationColumnCount = 18;

private static final int attachmentColumnCount = 10;

private static final int customerColumnCount = 2;

//private static final int deviceColumnCount = 26;

private static final int deviceAddressColumnCount = 10;

private static final int deviceISEEPersonColumnCount = 11;

private static final int deviceOOSIdentifiersColumnCount = 5;

private static final int valuePairColumnCount = 5;

private static String rootNodeName = "";

public static final String ISEE_COLLECTION_NAME_SPACE = "http://www.hp.com/schemas/isee/5.05/datacollection";
public static final String ISEE_EVENT_NAME_SPACE = "http://www.hp.com/schemas/isee/5.00/event";
public static final String HPS_COLLECTION_NAME_SPACE = "http://www.hp.com/schemas/support/6.00";
public static final String ISEE_METRICS_NAME_SPACE = "http://www.hp.com/schemas/isee/5.00/metrics";
public static final String[] rootElementNameList = new String[]{"partial_ISEE-Event", "//isee:ISEE-Event", 
	"//isee:ISEE-DataCollection", "//hps:DataCollection", "//isee:ISEE-Metrics"};

private List<ArrayList<String>> properties_agent_rule_code = new ArrayList<ArrayList<String>>();

private NodeList GetRootNode(Document doc, XPath xpath, String rootElementName) throws Exception
{		
	NodeList iseeEventChildNodes = null;
	try
	{
		if(rootElementName.equals("partial_ISEE-Event"))
		{
			XPathExpression expr = xpath.compile(rootElementName);		
			Object result = expr.evaluate(doc, XPathConstants.NODESET);    
			NodeList nodes = (NodeList) result;
		    if(nodes.getLength() != 0)
		    {
		        NamedNodeMap nameNodeMap = nodes.item(0).getAttributes();		
				if(nameNodeMap != null)
				{
					iseeEventChildNodes = nodes.item(0).getChildNodes();
					rootNodeName = rootElementName;
				}
		    }	    
		}
		else//isee:ISEE-DataCollection,isee:ISEE-Event,hps:DataCollection,isee:ISEE-Metrics
		{
		    NamespaceContext nsContext = new XHTMLNamespaceContext("");
			if(rootElementName.equals("//isee:ISEE-DataCollection"))		
				nsContext = new XHTMLNamespaceContext(ISEE_COLLECTION_NAME_SPACE);
			else if(rootElementName.equals("//isee:ISEE-Event"))
				nsContext = new XHTMLNamespaceContext(ISEE_EVENT_NAME_SPACE);
			else if(rootElementName.equals("//hps:DataCollection"))
				nsContext = new XHTMLNamespaceContext(HPS_COLLECTION_NAME_SPACE);
			else if(rootElementName.equals("//isee:ISEE-Metrics"))
				nsContext = new XHTMLNamespaceContext(ISEE_METRICS_NAME_SPACE);

		    xpath.setNamespaceContext(nsContext);
		    XPathExpression expr = xpath.compile(rootElementName);	    
		    Object result = expr.evaluate(doc, XPathConstants.NODESET);
		    NodeList nodes = (NodeList) result;
		    if(nodes.getLength() != 0)
		    {
		    	NamedNodeMap nameNodeMap = nodes.item(0).getAttributes();
		    	if(nameNodeMap != null)
		    	{
		    		iseeEventChildNodes = nodes.item(0).getChildNodes();
		    		rootNodeName = rootElementName;
		    	}
		    }
		}
		return iseeEventChildNodes;
	}
	catch(Exception e)
	{
		throw e;
	}
}

//under <HP_ISEEServiceIncident>
//<Business>XP Storage</Business>
//<AnalysisToolName>C-TRACK</AnalysisToolName>
//<Property name="RuleID" value="NSK_IncidentReport"/>
//<Property name="EventUniqueID" value="15135"/>
//<Property name="sim_ref_code" value="dcf301"/>
//<Description>PAIR VOLUME ST + AE191A_JPHA025138-RF=dcf301-DT=22 Jul 2014 05:29:56</Description>--ISEEServiceIncident
//<EventTypeId>dcf301</EventTypeId>
//<Caption>XP Storage SIMEVENT : EE8Z107</Caption>--ISEEServiceIncident
//under <CIM_AlertIndications>
//<EventID>dcf301</EventID>
public List<ArrayList<String>> ExtractXmlNodes(String xml, String zipFileName, String xmlFileName, String tableNames)throws Exception{
    //final list
	List<ArrayList<String>> finaloutput = new ArrayList<ArrayList<String>>();
	
	List<ArrayList<String>> alertindicationall = new ArrayList<ArrayList<String>>();//Fact_Stage_Common_Information_Model_Alert_Indication
	List<ArrayList<String>> deviceinformation = new ArrayList<ArrayList<String>>();//Fact_Stage_Device_Information
	List<ArrayList<String>> valuepair = new ArrayList<ArrayList<String>>();//Fact_Stage_Value_Pair
	
	//Separated list for alert indication table
	List<ArrayList<String>> serviceincident = new ArrayList<ArrayList<String>>();
	List<ArrayList<String>> alertindication = new ArrayList<ArrayList<String>>();
	List<ArrayList<String>> iseecustomer = new ArrayList<ArrayList<String>>();
	List<ArrayList<String>> attachment = new ArrayList<ArrayList<String>>();
	try{
		//compile the xml file to an object
		DocumentBuilderFactory domFactory = DocumentBuilderFactory.newInstance();		
		domFactory.setNamespaceAware(true);
		domFactory.setIgnoringElementContentWhitespace(true); 
	    domFactory.setIgnoringComments(true);	    
		DocumentBuilder builder = domFactory.newDocumentBuilder();
		Document doc = builder.parse(new InputSource(new StringReader(xml)));		
		XPathFactory factory = XPathFactory.newInstance();
	    XPath xpath = factory.newXPath();
	    
	    NodeList iseeEventChildNodes = null;
	    Boolean isprocess = false;
	    for(int i = 0; i< rootElementNameList.length; i++)
	    {
	    	iseeEventChildNodes = GetRootNode(doc, xpath, rootElementNameList[i].toString());
	    	if(iseeEventChildNodes != null) 
	    	{
	    		if(rootElementNameList[i].toString().equals("partial_ISEE-Event") ||
	    				rootElementNameList[i].toString().equals("//isee:ISEE-Event"))
	    			isprocess = true;
	    		break;
	    	}
	    }
	    /*
	    XPathExpression expr = xpath.compile("partial_ISEE-Event");
	    Object result = expr.evaluate(doc, XPathConstants.NODESET);
	    NodeList nodes = (NodeList) result;
	    
	    NamedNodeMap nameNodeMap = null;		
		NodeList iseeEventChildNodes = null;
	    if(nodes.getLength() != 0)
	    {
		    nameNodeMap = nodes.item(0).getAttributes();		
			if(nameNodeMap == null)return finaloutput;		
			iseeEventChildNodes = nodes.item(0).getChildNodes();
			rootNodeName = "partial_ISEE-Event";
	    }
	    else
	    {
		    NamespaceContext nsContext = new XHTMLNamespaceContext();
		    xpath.setNamespaceContext(nsContext);
		    expr = xpath.compile("//isee:ISEE-Event");	    
		    result = expr.evaluate(doc, XPathConstants.NODESET);
		    //get the node list from root node for processing
		    nodes = (NodeList) result;	    
		    nameNodeMap = nodes.item(0).getAttributes();		
			if(nameNodeMap == null)return finaloutput;		
			iseeEventChildNodes = nodes.item(0).getChildNodes();
			rootNodeName = "//isee:ISEE-Event";
	    }
	    */
	    if(!isprocess) return finaloutput; 
	    String createTimeStamp = GetCurrentDate();//create timestamp
	    //added at 2015/03/12 to add one flag to only read the fisrt attachment flag
	    boolean is_first_attachment_flag = true; 
		for(int i = 0; i < iseeEventChildNodes.getLength(); i++)
		{
			if(iseeEventChildNodes.item(i).getNodeType() == Node.ELEMENT_NODE)
			{
				if(iseeEventChildNodes.item(i).getNodeName().toUpperCase().equals("HP_ISEEServiceIncident".toUpperCase()))
				{
					TraverseServiceIncidentAll(serviceincident, valuepair, iseeEventChildNodes.item(i), doc, xpath, 1);
				}				
				else if(iseeEventChildNodes.item(i).getNodeName().toUpperCase().equals("CIM_AlertIndications".toUpperCase()))
				{
					TraverseServiceIncidentAll(alertindication, valuepair, iseeEventChildNodes.item(i), doc, xpath, 2);
				}				
				else if(iseeEventChildNodes.item(i).getNodeName().toUpperCase().equals("AlertSource".toUpperCase()))
				{
					//set device role code as "Alert Source" for AlertSource node
					TraverseDeviceAll(deviceinformation, valuepair, iseeEventChildNodes.item(i), true, zipFileName, xmlFileName, "Alert Source".toUpperCase());
				}				
				else if(iseeEventChildNodes.item(i).getNodeName().toUpperCase().equals("AssociatedDevice".toUpperCase()))
				{					
					NamedNodeMap nnm = iseeEventChildNodes.item(i).getAttributes();
					String deviceRoleCode = GetStringOrEmpty(nnm,"role");
					if(deviceRoleCode.equals("Hosting Device".toUpperCase()))
						TraverseDeviceAll(deviceinformation, valuepair, iseeEventChildNodes.item(i), false, zipFileName, xmlFileName, deviceRoleCode);
				}
				//updated at 2015/03/12 to add one flag to only read the fisrt attachment flag
				/*
				else if(iseeEventChildNodes.item(i).getNodeName().toUpperCase().equals("PRS_Attachment".toUpperCase()))
				{
					TraverseServiceIncidentAll(attachment, valuepair, iseeEventChildNodes.item(i), doc, xpath, 3);
				}*/
				else if(iseeEventChildNodes.item(i).getNodeName().toUpperCase().equals("PRS_Attachment".toUpperCase()) && is_first_attachment_flag)
				{
					TraverseServiceIncidentAll(attachment, valuepair, iseeEventChildNodes.item(i), doc, xpath, 3);
					is_first_attachment_flag = false;
				}
				else if(iseeEventChildNodes.item(i).getNodeName().toUpperCase().equals("HP_ISEECustomer".toUpperCase()))
				{
					TraverseServiceIncidentAll(iseecustomer, valuepair, iseeEventChildNodes.item(i), doc, xpath, 4);
				}
			}
		}		
		//get all stage table names
        String[] tableNameList = tableNames.split(",");
        if(tableNameList.length < 3) 
        {
        	LOG.info("mapred.vertica.tablenames parameter is invalid!");
        	LOG.info("value pair records of current xml file: "+valuepair.size());
        	return finaloutput;
        }        
		//make up data of Common_Information_Model_Alert_Indication table - 45 columns + tablename
		//serviceincident,alertindication,iseecustomer,attachment
        alertindicationall = GetServiceIncidentStageTableRecord(xmlFileName, zipFileName,
				serviceincident, alertindication, iseecustomer, attachment);
		for(ArrayList<String> al:alertindicationall)
		{
			ArrayList<String> al2 = new ArrayList<String>();
			al2.add(tableNameList[0].toString());
			al2.add(createTimeStamp);
			al2.addAll(al);
			finaloutput.add(al2);
		}
		
		//make up data of Device_Information table - 29 columns + tablename
		for(ArrayList<String> al:deviceinformation)
		{
			ArrayList<String> al2 = new ArrayList<String>();
			al2.add(tableNameList[1].toString());
			al2.add(createTimeStamp);
			al2.addAll(al);
			finaloutput.add(al2);
		}		
		//make up data of Value_Pair table - 6 columns + tablename
		for(ArrayList<String> al:valuepair)
		{
			ArrayList<String> al2 = new ArrayList<String>();
			al2.add(tableNameList[2].toString());
			al2.add(createTimeStamp);
			al2.add(xmlFileName);
			al2.addAll(al);
			finaloutput.add(al2);
		}
	}
	catch (Exception e) {
		LOG.info("ExtractNodes Error: "+ e.getMessage());
		throw new Exception(e);
	}
	return finaloutput;	
}

//OOSProperties tag in both Alert Source and Associated Device nodes
public List<ArrayList<String>> ExtractXmlNodesForHistoryDataLoad(String xml, String zipFileName, String xmlFileName, String tableName)throws Exception{
    //final list
	List<ArrayList<String>> finaloutput = new ArrayList<ArrayList<String>>();
	List<ArrayList<String>> valuepair = new ArrayList<ArrayList<String>>();//Fact_Stage_Value_Pair
	try{
		//compile the xml file to an object
		DocumentBuilderFactory domFactory = DocumentBuilderFactory.newInstance();		
		domFactory.setNamespaceAware(true);
		domFactory.setIgnoringElementContentWhitespace(true); 
	    domFactory.setIgnoringComments(true);	    
		DocumentBuilder builder = domFactory.newDocumentBuilder();
		Document doc = builder.parse(new InputSource(new StringReader(xml)));		
		XPathFactory factory = XPathFactory.newInstance();
	    XPath xpath = factory.newXPath();
	    
	    NodeList iseeEventChildNodes = null;
	    Boolean isprocess = false;
	    for(int i = 0; i< rootElementNameList.length; i++)
	    {
	    	iseeEventChildNodes = GetRootNode(doc, xpath, rootElementNameList[i].toString());
	    	if(iseeEventChildNodes != null) 
	    	{
	    		if(rootElementNameList[i].toString().equals("partial_ISEE-Event") ||
	    				rootElementNameList[i].toString().equals("//isee:ISEE-Event"))
	    			isprocess = true;
	    		break;
	    	}
	    }
	    if(!isprocess) return finaloutput; 
	    String createTimeStamp = GetCurrentDate();//create timestamp
		for(int i = 0; i < iseeEventChildNodes.getLength(); i++)
		{
			if(iseeEventChildNodes.item(i).getNodeType() == Node.ELEMENT_NODE &&
					iseeEventChildNodes.item(i).getNodeName().toUpperCase().equals("AlertSource".toUpperCase()))
			{
				TraverseDeviceAllForHistoryDataLoad(valuepair, iseeEventChildNodes.item(i), true, "Alert Source".toUpperCase());
			}
			else if(iseeEventChildNodes.item(i).getNodeType() == Node.ELEMENT_NODE &&
					iseeEventChildNodes.item(i).getNodeName().toUpperCase().equals("AssociatedDevice".toUpperCase()))
			{
				NamedNodeMap nnm = iseeEventChildNodes.item(i).getAttributes();
				String deviceRoleCode = GetStringOrEmpty(nnm,"role");
				if(deviceRoleCode.equals("Hosting Device".toUpperCase()))
				TraverseDeviceAllForHistoryDataLoad(valuepair, iseeEventChildNodes.item(i), false, deviceRoleCode);
			}
		}      	
		//make up data of Value_Pair table - 6 columns + tablename
		for(ArrayList<String> al:valuepair)
		{
			ArrayList<String> al2 = new ArrayList<String>();
			al2.add(tableName.toString());
			al2.add(createTimeStamp);
			al2.add(xmlFileName);
			al2.addAll(al);
			finaloutput.add(al2);
		}
	}
	catch (Exception e) {
		LOG.info("ExtractNodes Error: "+ e.getMessage());
		throw new Exception(e);
	}
	return finaloutput;	
}
private void TraverseDeviceAllForHistoryDataLoad(List<ArrayList<String>> valuepairoutput, Node node, boolean isAlertSource, String deviceRoleCode)
{
	NodeList childNodes = node.getChildNodes();
	if (childNodes==null || childNodes.getLength()== 0) {return;}
	int oosProperties = 1;
	for(int i= 0; i < childNodes.getLength(); i++)
	{
		if(childNodes.item(i).getNodeType() == Node.ELEMENT_NODE  && isAlertSource &&
				childNodes.item(i).getNodeName().toUpperCase().equals("OBJECTOFSERVICEPROPERTIES"))
		{
    		TraverseValuePair(valuepairoutput, childNodes.item(i), oosProperties);
    		oosProperties++;
		}
		else if(childNodes.item(i).getNodeType() == Node.ELEMENT_NODE  && 
				!isAlertSource && deviceRoleCode.equals("Hosting Device".toUpperCase()) &&
				childNodes.item(i).getNodeName().toUpperCase().equals("OBJECTOFSERVICEPROPERTIES"))
		{
    		TraverseValuePair(valuepairoutput, childNodes.item(i), oosProperties);
    		oosProperties++;
		}
	}
}

private void TraverseDeviceAll(List<ArrayList<String>> output, List<ArrayList<String>> valuepairoutput, Node node, boolean isAlertSource, 
		String zipFileName, String xmlFileName, String deviceRoleCode)
{
	List<ArrayList<String>> singleDeviceRecord = new ArrayList<ArrayList<String>>();
	NodeList childNodes = node.getChildNodes();
	if (childNodes==null || childNodes.getLength()==0) {return;}
	
	//Separated list for device information table
	List<ArrayList<String>> address = new ArrayList<ArrayList<String>>();
	List<ArrayList<String>> person = new ArrayList<ArrayList<String>>();
	List<ArrayList<String>> oosidentifiers = new ArrayList<ArrayList<String>>();
	int alertSourcePersonIndex = 1;
	int associatedDevicePersonIndex = 1;
	int oosProperties = 1;
	for(int i= 0; i < childNodes.getLength(); i++)
	{
		if(childNodes.item(i).getNodeType() == Node.ELEMENT_NODE)
		{
			String nodeName = childNodes.item(i).getNodeName().toUpperCase();
			if(nodeName.equals("PRS_ADDRESS"))
				TraverseDeviceNodes(address, valuepairoutput, childNodes.item(i), 1, 1);
			else if(nodeName.equals("HP_ISEEPERSON"))
			{
				if(isAlertSource)
			    {		
			    	TraverseDeviceNodes(person, valuepairoutput, childNodes.item(i), 2, alertSourcePersonIndex);
			    	alertSourcePersonIndex++;
			    }
			    else
			    {
			    	TraverseDeviceNodes(person, valuepairoutput, childNodes.item(i), 2, associatedDevicePersonIndex);
			    	associatedDevicePersonIndex++;
			    }	
			}
			else if(nodeName.equals("HP_OOSIDENTIFIERS"))
				TraverseDeviceNodes(oosidentifiers, valuepairoutput, childNodes.item(i), 3, 1);			
			//new requirement for getting properties in ObjectOfServiceProperties to value pair table
			else if(nodeName.equals("OBJECTOFSERVICEPROPERTIES"))
			{
	    		TraverseValuePair(valuepairoutput, childNodes.item(i), oosProperties);
	    		oosProperties++;
			}			
		}
	}
	singleDeviceRecord = GetDeviceStageTableRecord(xmlFileName, zipFileName, address, person, oosidentifiers);
	for(ArrayList<String> al:singleDeviceRecord)
	{
		ArrayList<String> al2 = new ArrayList<String>();
		al2.add(deviceRoleCode);
		al2.addAll(al);
		output.add(al2);
	}
}

private void TraverseDeviceNodes(List<ArrayList<String>> output, List<ArrayList<String>> valuepairoutput, Node node, int number, int personIndex)
{
	if(number == 1)
	{
		ArrayList<String> address = new ArrayList<String>(deviceAddressColumnCount);
		InitialArray(address, deviceAddressColumnCount);
	    NodeList childNodes = node.getChildNodes();
	    if (childNodes == null || childNodes.getLength() == 0) {return;}
	    for(int i = 0; i < childNodes.getLength(); i++)
	    {
	    	if(childNodes.item(i).getNodeType() == Node.ELEMENT_NODE)
	    	{	   
	    		String nodeName = childNodes.item(i).getNodeName().toUpperCase();
		    	if(nodeName.equals("AddressType".toUpperCase()))
		        	address.set(0, childNodes.item(i).getTextContent());
		    	else if(nodeName.equals("Address1".toUpperCase()))
			    	address.set(1, childNodes.item(i).getTextContent());
		    	else if(nodeName.equals("Address2".toUpperCase()))	    
		        	address.set(2, childNodes.item(i).getTextContent());
		    	else if(nodeName.equals("Address3".toUpperCase()))	    	
		        	address.set(3, childNodes.item(i).getTextContent());
		    	else if(nodeName.equals("Address4".toUpperCase()))
		        	address.set(4, childNodes.item(i).getTextContent());
		    	else if(nodeName.equals("City".toUpperCase()))
			    	address.set(5, childNodes.item(i).getTextContent());
		    	else if(nodeName.equals("Region".toUpperCase()))
			    	address.set(6, childNodes.item(i).getTextContent());
		    	else if(nodeName.equals("PostalCode".toUpperCase()))
			    	address.set(7, childNodes.item(i).getTextContent());
		    	else if(nodeName.equals("TimeZone".toUpperCase()))
			    	address.set(8, childNodes.item(i).getTextContent());
		    	else if(nodeName.equals("Country".toUpperCase()))
			    	address.set(9, childNodes.item(i).getTextContent());
	    	}
	    }
	    output.add(address);
	}
	else if(number == 2)
	{
		ArrayList<String> person = new ArrayList<String>(deviceISEEPersonColumnCount);
		InitialArray(person, deviceISEEPersonColumnCount);
	    NodeList childNodes = node.getChildNodes();
	    if (childNodes == null || childNodes.getLength() == 0) {return;}
	    for(int i = 0; i < childNodes.getLength(); i++)
	    {
	    	if(childNodes.item(i).getNodeType() == Node.ELEMENT_NODE)
	    	{
				String nodeName = childNodes.item(i).getNodeName().toUpperCase();
		    	if(nodeName.equals("CommunicationMode".toUpperCase()))
			    	person.set(0, childNodes.item(i).getTextContent());
		    	else if(nodeName.equals("ContactType".toUpperCase()))
			    	person.set(1, childNodes.item(i).getTextContent());
		    	else if(nodeName.equals("FirstName".toUpperCase())) 
			    	person.set(2, childNodes.item(i).getTextContent());
		    	else if(nodeName.equals("LastName".toUpperCase()))
			    	person.set(3, childNodes.item(i).getTextContent());
		    	else if(nodeName.equals("Salutation".toUpperCase())) 
			    	person.set(4, childNodes.item(i).getTextContent());
		    	else if(nodeName.equals("Title".toUpperCase())) 
			    	person.set(5, childNodes.item(i).getTextContent());
		    	else if(nodeName.equals("EmailAddress".toUpperCase())) 
			    	person.set(6, childNodes.item(i).getTextContent());
		    	else if(nodeName.equals("TelephoneNumber".toUpperCase())) 
			    	person.set(7, childNodes.item(i).getTextContent());
		    	else if(nodeName.equals("PreferredLanguage".toUpperCase())) 
			    	person.set(8, childNodes.item(i).getTextContent());
		    	else if(nodeName.equals("Availability".toUpperCase()))
			    	person.set(9, childNodes.item(i).getTextContent());	
	    	}
	    }
	    person.set(10, String.valueOf(personIndex));
	    output.add(person);
	}
	else if(number == 3)
	{
		ArrayList<String> oosid = new ArrayList<String>(deviceOOSIdentifiersColumnCount);
		InitialArray(oosid, deviceOOSIdentifiersColumnCount);
	    NodeList childNodes = node.getChildNodes();
	    if (childNodes == null || childNodes.getLength() == 0) {return;}
		int sectionIndex = 1;
	    for(int i = 0; i < childNodes.getLength(); i++)
	    {
	    	if(childNodes.item(i).getNodeType() == Node.ELEMENT_NODE)
	    	{
				String nodeName = childNodes.item(i).getNodeName().toUpperCase();
		    	if(nodeName.equals("GDID"))
		    	{
		    		NamedNodeMap nameNodeMap = childNodes.item(i).getAttributes();
		    		String gdidType = GetStringOrEmpty(nameNodeMap, "type");
	    			String gdidUsn = GetStringOrEmpty(nameNodeMap, "usn");
	    			String gdidValue = childNodes.item(i).getTextContent();
		    		if(gdidType.equals("HARDWARE"))
		    		{
		    			oosid.set(0,gdidUsn);
		    			oosid.set(1,gdidValue);
		    		}
		    		else if(gdidType.equals("OPERATINGSYSTEM"))
		    		{
		    			oosid.set(2,gdidUsn);
		    			oosid.set(3,gdidValue);
		    		}
		    	}
		    	else if(nodeName.equals("LDID")) 
			    	oosid.set(4, childNodes.item(i).getTextContent());
		    	else if(nodeName.equals("CSID")) 
		    	{		    		
		    	    NodeList childchildNodes = childNodes.item(i).getChildNodes();
		    	    if (childchildNodes == null || childchildNodes.getLength() == 0) {break;}
		    	    else
		    	    {	
		    		    for(int j = 0; j < childchildNodes.getLength(); j++)
		    		    {
		    		    	if(childchildNodes.item(j).getNodeType() == Node.ELEMENT_NODE)
		    		    	{
		    		    		TraverseValuePair(valuepairoutput, childchildNodes.item(j), sectionIndex);
				    	    	sectionIndex++;
		    		    	}
		    		    }	    	    	
		    	    }		    	    
		    	}
	    	}
	    }
	    output.add(oosid);
	}
}

private List<ArrayList<String>> GetDeviceStageTableRecord(String xmlFileName, String zipFileName,
		List<ArrayList<String>> address, List<ArrayList<String>> person, List<ArrayList<String>> oosidentifiers)
{
	List<ArrayList<String>> result = new ArrayList<ArrayList<String>>();
	boolean add = false;
	if(address.size() == 0)
	{
		ArrayList<String> addr = new ArrayList<String>(deviceAddressColumnCount);
		InitialArray(addr,deviceAddressColumnCount);
		address.add(addr);			
	}
	else 
		add = true;
	if(person.size() == 0)
	{
		ArrayList<String> pern = new ArrayList<String>(deviceISEEPersonColumnCount);
		InitialArray(pern,deviceISEEPersonColumnCount);
		//set person index default value as 1
		pern.set(deviceISEEPersonColumnCount-1, "1");
		person.add(pern);
	}
	else 
		add = true;
	if(oosidentifiers.size() == 0)
	{
		ArrayList<String> oosid = new ArrayList<String>(deviceOOSIdentifiersColumnCount);
		InitialArray(oosid,deviceOOSIdentifiersColumnCount);
		oosidentifiers.add(oosid);			
	}
	else 
		add = true;

	if(add)
	{
		for(int i = 0; i < person.size(); i++)
		{
			//add data
			ArrayList<String> al = new ArrayList<String>();
			al.add(xmlFileName);
			al.addAll(address.get(0));
			al.addAll(person.get(i));
			al.addAll(oosidentifiers.get(0));
			//al.add(zipFileName);
			result.add(al);
		}
	}	
	return result;
}


@SuppressWarnings("unused")
private String GetNodeValue(NamedNodeMap nameNodeMap, String childNodeName)
{
	String result = (nameNodeMap.getNamedItem(childNodeName).getNodeValue() == null? "" : nameNodeMap.getNamedItem(childNodeName).getNodeValue()).toString();
	return result;
}

private void TraverseServiceIncidentAll(List<ArrayList<String>> output, List<ArrayList<String>> valuepairoutput, Node node, Document doc, XPath xpath, int number) throws XPathExpressionException, DOMException
{	
	if(number == 1)
	{
	    NodeList childNodes = node.getChildNodes();
	    if (childNodes==null || childNodes.getLength()==0) {return;}
		ArrayList<String> serviceinct = new ArrayList<String>(serviceIncidentColumnCount);
		InitialArray(serviceinct,serviceIncidentColumnCount);
		serviceinct.set(0,ExtractTag(doc,xpath,rootNodeName + "/HP_ISEEServiceIncident/Caption"));
		serviceinct.set(1,ExtractTag(doc,xpath,rootNodeName + "/HP_ISEEServiceIncident/Description"));
		serviceinct.set(2,ExtractTag(doc,xpath,rootNodeName + "/HP_ISEEServiceIncident/CurrentState"));
		serviceinct.set(3,ExtractTag(doc,xpath,rootNodeName + "/HP_ISEEServiceIncident/Entitled"));
		serviceinct.set(4,ExtractTag(doc,xpath,rootNodeName + "/HP_ISEEServiceIncident/ProviderID"));
		serviceinct.set(5,ExtractTag(doc,xpath,rootNodeName + "/HP_ISEEServiceIncident/RequesterID"));
		serviceinct.set(6,ExtractTag(doc,xpath,rootNodeName + "/HP_ISEEServiceIncident/SequenceId"));
		serviceinct.set(7,ExtractTag(doc,xpath,rootNodeName + "/HP_ISEEServiceIncident/Severity"));
		serviceinct.set(8,ExtractTag(doc,xpath,rootNodeName + "/HP_ISEEServiceIncident/EventTime"));
		serviceinct.set(9,ExtractTag(doc,xpath,rootNodeName + "/HP_ISEEServiceIncident/EventTypeId"));
		serviceinct.set(10,ExtractTag(doc,xpath,rootNodeName + "/HP_ISEEServiceIncident/Business"));
		serviceinct.set(11,ExtractTag(doc,xpath,rootNodeName + "/HP_ISEEServiceIncident/AnalysisToolName"));		
		serviceinct.set(12,ExtractTag(doc,xpath,rootNodeName + "/HP_ISEEServiceIncident/OperationStatus"));
		if(!(serviceinct.isEmpty() || serviceinct.toArray().length == 0))
			output.add(serviceinct);
	    
	    TraverseValuePair(valuepairoutput, node, 0);
	    //can get from value pair  as well
	    GetAgentRuleCodeProperties(node);
	}
	else if(number == 2)
	{
	    NodeList childNodes = node.getChildNodes();
	    if (childNodes==null || childNodes.getLength()==0) {return;}
		ArrayList<String> alertindn = new ArrayList<String>(alertIndicationColumnCount);
		InitialArray(alertindn,alertIndicationColumnCount);
	    alertindn.set(0,ExtractTag(doc,xpath,rootNodeName + "/CIM_AlertIndications/CIM_AlertIndication/IndicationIdentifier"));
	    alertindn.set(1,ExtractTag(doc,xpath,rootNodeName + "/CIM_AlertIndications/CIM_AlertIndication/CorrelatedIndications"));
	    alertindn.set(2,ExtractTag(doc,xpath,rootNodeName + "/CIM_AlertIndications/CIM_AlertIndication/Description"));
	    alertindn.set(3,ExtractTag(doc,xpath,rootNodeName + "/CIM_AlertIndications/CIM_AlertIndication/AlertingManagedElement"));
	    alertindn.set(4,ExtractTag(doc,xpath,rootNodeName + "/CIM_AlertIndications/CIM_AlertIndication/AlertingElementFormat"));
	    alertindn.set(5,ExtractTag(doc,xpath,rootNodeName + "/CIM_AlertIndications/CIM_AlertIndication/OtherAlertingElementFormat"));
	    alertindn.set(6,ExtractTag(doc,xpath,rootNodeName + "/CIM_AlertIndications/CIM_AlertIndication/AlertType"));
	    alertindn.set(7,ExtractTag(doc,xpath,rootNodeName + "/CIM_AlertIndications/CIM_AlertIndication/OtherAlertType"));
	    alertindn.set(8,ExtractTag(doc,xpath,rootNodeName + "/CIM_AlertIndications/CIM_AlertIndication/PerceivedSeverity"));
	    alertindn.set(9,ExtractTag(doc,xpath,rootNodeName + "/CIM_AlertIndications/CIM_AlertIndication/OtherSeverity"));
	    alertindn.set(10,ExtractTag(doc,xpath,rootNodeName + "/CIM_AlertIndications/CIM_AlertIndication/RecommendedActions"));
	    alertindn.set(11,ExtractTag(doc,xpath,rootNodeName + "/CIM_AlertIndications/CIM_AlertIndication/ProbableCause"));
	    alertindn.set(12,ExtractTag(doc,xpath,rootNodeName + "/CIM_AlertIndications/CIM_AlertIndication/ProbableCauseDescription"));
	    alertindn.set(13,ExtractTag(doc,xpath,rootNodeName + "/CIM_AlertIndications/CIM_AlertIndication/EventTime"));
	    alertindn.set(14,ExtractTag(doc,xpath,rootNodeName + "/CIM_AlertIndications/CIM_AlertIndication/SystemCreationClassName"));
	    alertindn.set(15,ExtractTag(doc,xpath,rootNodeName + "/CIM_AlertIndications/CIM_AlertIndication/SystemName"));
	    alertindn.set(16,ExtractTag(doc,xpath,rootNodeName + "/CIM_AlertIndications/CIM_AlertIndication/ProviderName"));
	    alertindn.set(17,ExtractTag(doc,xpath,rootNodeName + "/CIM_AlertIndications/CIM_AlertIndication/EventID"));
	    if(!(alertindn.isEmpty() || alertindn.toArray().length == 0))
	    	output.add(alertindn);
	    
	    TraverseValuePair(valuepairoutput, node, 0);
	    //AlertIndications has a child tag then it goes to properties
	    NodeList childchildNodes = node.getChildNodes();
	    if (childchildNodes==null || childchildNodes.getLength()==0) {return;}
	    TraverseValuePair(valuepairoutput, childchildNodes.item(0), 0);
	}
	else if(number == 3)
	{
	    NodeList childNodes = node.getChildNodes();
	    if (childNodes==null || childNodes.getLength()==0) {return;}
		ArrayList<String> attachment = new ArrayList<String>(attachmentColumnCount);
		InitialArray(attachment,attachmentColumnCount);
	    attachment.set(0,ExtractTag(doc,xpath,rootNodeName + "/PRS_Attachment/Caption"));
	    attachment.set(1,ExtractTag(doc,xpath,rootNodeName + "/PRS_Attachment/Description"));
	    attachment.set(2,ExtractTag(doc,xpath,rootNodeName + "/PRS_Attachment/DirtyFlag"));
	    attachment.set(3,ExtractTag(doc,xpath,rootNodeName + "/PRS_Attachment/AttachmentName"));
	    attachment.set(4,ExtractTag(doc,xpath,rootNodeName + "/PRS_Attachment/AttachmentObject"));
	    attachment.set(5,ExtractTag(doc,xpath,rootNodeName + "/PRS_Attachment/AttachmentReference"));
	    attachment.set(6,ExtractTag(doc,xpath,rootNodeName + "/PRS_Attachment/AttachmentSize"));
	    attachment.set(7,ExtractTag(doc,xpath,rootNodeName + "/PRS_Attachment/AttachmentType"));
	    attachment.set(8,ExtractTag(doc,xpath,rootNodeName + "/PRS_Attachment/Format"));
	    attachment.set(9,ExtractTag(doc,xpath,rootNodeName + "/PRS_Attachment/Protocol"));
	    if(!(attachment.isEmpty() || attachment.toArray().length == 0))
	    	output.add(attachment);
	    
	    TraverseValuePair(valuepairoutput, node, 0);
	}
	else if(number == 4)
	{
	    NodeList childNodes = node.getChildNodes();
	    if (childNodes==null || childNodes.getLength()==0) {return;}
		ArrayList<String> customer = new ArrayList<String>(customerColumnCount);
		InitialArray(customer,customerColumnCount);
	    customer.set(0,ExtractTag(doc,xpath,rootNodeName + "/HP_ISEECustomer/Business"));
	    customer.set(1,ExtractTag(doc,xpath,rootNodeName + "/HP_ISEECustomer/Name"));
	    if(!(customer.isEmpty() || customer.toArray().length == 0))
	    	output.add(customer);
	    
	    TraverseValuePair(valuepairoutput, node, 0);
	}	
}

private void GetAgentRuleCodeProperties(Node node)
{
	ArrayList<String> property = new ArrayList<String>(3);
	InitialArray(property, 3);
    NodeList childNodes = node.getChildNodes();
    if (childNodes.getLength() != 0 || childNodes != null) 
    {    
        for(int s = 0; s < childNodes.getLength(); s++)
        {
            if(childNodes.item(s).getNodeType() == Node.ELEMENT_NODE && childNodes.item(s).getNodeName().equals("Property"))
        	{
        		NamedNodeMap properetyMap = childNodes.item(s).getAttributes();
        		if(GetStringOrEmpty(properetyMap,"name").equals("RuleID".toUpperCase()))
        			property.set(0, GetStringOrEmpty(properetyMap,"value"));
        		if(GetStringOrEmpty(properetyMap,"name").equals("EventUniqueID".toUpperCase()))
        			property.set(1, GetStringOrEmpty(properetyMap,"value"));
        		if(GetStringOrEmpty(properetyMap,"name").equals("sim_ref_code".toUpperCase()))
        			property.set(2, GetStringOrEmpty(properetyMap,"value"));
        	}        	
        }
    }
    properties_agent_rule_code.add(property);
}

private List<ArrayList<String>> GetServiceIncidentStageTableRecord(String xmlFileName, String zipFileName,
		List<ArrayList<String>> serviceIncident, List<ArrayList<String>> alertIndication,
		List<ArrayList<String>> iseeCustomer,List<ArrayList<String>> attachment)
{
	List<ArrayList<String>> result = new ArrayList<ArrayList<String>>();
	boolean add = false;
	if(serviceIncident.size() == 0)
	{
		ArrayList<String> serviceinct = new ArrayList<String>(serviceIncidentColumnCount);
		InitialArray(serviceinct,serviceIncidentColumnCount);
		serviceIncident.add(serviceinct);			
	}
	else 
		add = true;
	if(alertIndication.size() == 0)
	{
		ArrayList<String> alertindn = new ArrayList<String>(alertIndicationColumnCount);
		InitialArray(alertindn,alertIndicationColumnCount);
		alertIndication.add(alertindn);		
	}
	else 
		add = true;
	if(attachment.size() == 0)
	{
		ArrayList<String> attachmentarray = new ArrayList<String>(attachmentColumnCount);
		InitialArray(attachmentarray,attachmentColumnCount);
		attachment.add(attachmentarray);			
	}
	else 
		add = true;
	if(iseeCustomer.size() == 0)
	{
		ArrayList<String> customer = new ArrayList<String>(customerColumnCount);
		InitialArray(customer,customerColumnCount);
		iseeCustomer.add(customer);			
	}
	else 
		add = true;
	if(add)
	{
		for(int i = 0; i < alertIndication.size(); i++)
		{
			//get Agent_Rule_Code			
			String agentRuleCode = "";			
			if(serviceIncident.size() != 0 && alertIndication.size() == 0)
			{
				agentRuleCode = GetAgentRuleCode(serviceIncident.get(i), alertIndication.get(i), false, i);
			}
			else if (serviceIncident.size() !=0 && alertIndication.size() != 0)
			{
				agentRuleCode = GetAgentRuleCode(serviceIncident.get(i), alertIndication.get(i), true, i);
			}
			//add data
			ArrayList<String> al = new ArrayList<String>();
			//al.add(tableName);
			al.add(xmlFileName);
			//remove event id from alert indication array
			alertIndication.get(i).remove(17);	
			al.addAll(alertIndication.get(i));
			al.addAll(serviceIncident.get(i));
			al.addAll(attachment.get(i));
			al.addAll(iseeCustomer.get(i));
			al.add(agentRuleCode);
			al.add(zipFileName);
			result.add(al);
		}
		properties_agent_rule_code.clear();
	}	
	return result;
}

@SuppressWarnings("unused")
private  void CopyTuple(ArrayList<String> source,ArrayList<String> target,int start,int end)
{
	for(int i = start;i<end;i++)
		try {
			target.set(i, source.get(i));
		} catch (Exception e) {
			e.printStackTrace();
		}
}

private String GetStringOrEmpty(NamedNodeMap nnm,String attr)
{
	return nnm.getNamedItem(attr)==null?"":nnm.getNamedItem(attr).getNodeValue().toUpperCase();
}

public String CombineArray2String(ArrayList<String> al)
{
	StringBuilder sb = new StringBuilder();
	for(int i=0;i<al.size()-1;i++)
	{
		sb.append(al.get(i));
		sb.append('\002') ;
	}
	sb.append(al.get(al.size()-1));
	return sb.toString();
}

private void InitialArray(ArrayList<String> al, int count)
{
	for( int i = 0;i < count; i++ )
		al.add("");
}

public String ExtractTag(Document doc,XPath xpath ,String tag)
{
	String result="";
	XPathExpression expr;
	try {
		 expr = xpath.compile(tag);
		 Object obj = expr.evaluate(doc, XPathConstants.NODE);
		 Node node = (Node) obj;
		 if(node!=null)
			 result = node.getTextContent();
	} catch (XPathExpressionException e) {
		e.printStackTrace();
	} 
	 return result;	 
}

private void TraverseValuePair(List<ArrayList<String>> output, Node node, int sectionIndex)
{
    NodeList childNodes = node.getChildNodes();
    if (childNodes==null || childNodes.getLength()==0) {return;}
    
	String paringCode = GetParingCode(node);
	String sectionName = GetSectionName(node);
    for(int s = 0; s < childNodes.getLength(); s++)
    {
    	/*Paring_code          varchar(40)  NULL ,
    	Property_Name        varchar(252)  NULL ,
    	Section_Name         varchar(252)  NULL ,
    	Value_Description    varchar(65000)  NULL ,
    	Section_Index_Count  integer  NULL */
        if(childNodes.item(s).getNodeType() == Node.ELEMENT_NODE && childNodes.item(s).getNodeName().equals("Property"))
    	{
        	ArrayList<String> valuepair = new ArrayList<String>(valuePairColumnCount);
        	InitialArray(valuepair,valuePairColumnCount);
    		NamedNodeMap properetyMap = childNodes.item(s).getAttributes();
    		valuepair.set(0, paringCode);
    		valuepair.set(1, GetStringOrEmpty(properetyMap,"name"));
    		valuepair.set(2, sectionName);
    		valuepair.set(3, GetStringOrEmpty(properetyMap,"value"));
    		valuepair.set(4, String.valueOf(sectionIndex));
    		output.add(valuepair);
    	}        	
    }
}

private String GetParingCode(Node node)
{
	String paringCode = node.getNodeName();
	Node parentNode = node.getParentNode();
	String rootnodename = rootNodeName;
	rootnodename = rootnodename.replaceAll("//", "");
	String rootnodenamewithoutnamespace = "";
	if(rootnodename.contains(":"))
		rootnodenamewithoutnamespace = rootnodename.trim().split(":")[1];
	while(!parentNode.getNodeName().toUpperCase().equals(rootnodename.trim().toUpperCase()) &&
			!parentNode.getNodeName().toUpperCase().equals(rootnodenamewithoutnamespace.trim().toUpperCase()))
	{
		paringCode += "/" + parentNode.getNodeName();
		parentNode = parentNode.getParentNode();
	}
	/*
	if(rootNodeName.equals("partial_ISEE-Event"))
	{
		while(!parentNode.getNodeName().toUpperCase().equals("partial_ISEE-Event".toUpperCase()))
		{
			paringCode += "/" + parentNode.getNodeName();
			parentNode = parentNode.getParentNode();
		}
	}
	else if(rootNodeName.equals("//isee:ISEE-Event"))
	{
		while(!parentNode.getNodeName().toUpperCase().equals("isee:ISEE-Event".toUpperCase()) &&
				!parentNode.getNodeName().toUpperCase().equals("ISEE-Event".toUpperCase()))
		{
			paringCode += "/" + parentNode.getNodeName();
			parentNode = parentNode.getParentNode();
		}
	}
	*/
	return paringCode;
}

private String GetSectionName(Node node)
{
	String sectionName ="-1";
	if(node.getNodeName().toUpperCase().equals("SECTION"))
	{
		NamedNodeMap properetyMap = node.getAttributes();
		sectionName = GetStringOrEmpty(properetyMap,"name");
	}
	return sectionName;
}

@SuppressWarnings("finally")
private String GetSubString(String source, String partition)
{
	String result = "";
	String[] sourceList = source.split(partition);
	try{
	if(sourceList.length > 1 && sourceList[1].contains("("))
		//HSV_SCMI_Rule --  V 1.20    Event Code: (09 3E 42 0E)
		result = sourceList[1].trim().substring(sourceList[1].trim().indexOf("(") + 1, sourceList[1].trim().indexOf(")"));
	else if(sourceList.length > 1 && !sourceList[1].contains("("))
		//HSV_SDC_Diagnostics -- V 1.14    Event Code:  0E 04 C8 19
		result = sourceList[1].trim();
	}catch(Exception e){}
	finally{
	return result;
	}
}

private String GetAgentRuleCode(ArrayList<String> serviceIncident, ArrayList<String> alertIndication, boolean isExist, int index)
{
	String agent_rule_code = "";
	//properties to get agent_rule_code
	 String business = serviceIncident.get(10);
	 String analysis_tool_name = serviceIncident.get(11);
	 String description = serviceIncident.get(1);
	 String event_type_id = serviceIncident.get(9);
	 String caption = serviceIncident.get(0);
	 String event_id = "";
	 if(isExist)
		 event_id = alertIndication.get(17);
	 String rule_id = "";
	 String event_unique_id = "";
	 String sim_ref_code = "";
	 if(properties_agent_rule_code.size() > 0)
	 {
		 rule_id = properties_agent_rule_code.get(index).get(0);
		 event_unique_id = properties_agent_rule_code.get(index).get(1);
		 sim_ref_code = properties_agent_rule_code.get(index).get(2);
	 }
	 if(business.toUpperCase().equals("EVA"))
	 {
		if(rule_id.equals(""))
			 agent_rule_code = "NotParsed";
		else if(rule_id.toUpperCase().contains("Event Code".toUpperCase()))	
			agent_rule_code = GetSubString(rule_id, "Event Code:".toUpperCase());
		else 
			agent_rule_code = rule_id;
	 }
	 else if(business.toUpperCase().equals("Nonstop".toUpperCase()) && !event_unique_id.equals(""))
	 {
		 agent_rule_code = event_unique_id;
	 }
	 else 
	 {
		 if(!rule_id.equals(""))
			 agent_rule_code = rule_id;
		 else
		 {
			 if(!sim_ref_code.equals(""))
			 {
				 if(sim_ref_code.length() > 4)
					 agent_rule_code = sim_ref_code.substring(0,4);
				 else
					 agent_rule_code = sim_ref_code;
			 }
			 else
			 {
				 if(analysis_tool_name.toUpperCase().equals("EMS") && !event_id.equals(""))
					 agent_rule_code = event_id;
				 else
				 {
					 if(business.toUpperCase().equals("gretsky_AIX".toUpperCase()) && !description.equals("") 
							 && description.toUpperCase().contains("Msgid".toUpperCase()))
						 //TBD - regular expression
						 agent_rule_code = description;
					 else
					 {
						 if(business.toUpperCase().equals("MCPS") && !event_type_id.equals(""))
							 agent_rule_code = event_type_id;
						 else
						 {
							 if(caption.equals(""))
								 agent_rule_code = "NotParsed";
							 else
								 agent_rule_code = caption;
						 }
					 }

				 }
			 }
		 }
	 }
	 return agent_rule_code;
}

public String GetCurrentDate()
{
	SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	return f.format(new java.util.Date());
}
}