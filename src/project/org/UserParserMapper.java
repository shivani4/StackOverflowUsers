package project.org;

import java.io.IOException;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.HashSet;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.nio.charset.CharacterCodingException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.xml.sax.InputSource;
import org.w3c.dom.*;
import javax.xml.xpath.*;
import java.io.StringReader;
import org.apache.hadoop.mapreduce.lib.input.*;

public class UserParserMapper extends Mapper<LongWritable, Text, Text,Text >{
	
	
	@Override
    	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {        
	
		String id="";
		String displayName=""; 
		try{
			Element rowe= getRowElement(value);
			id=getRowId(rowe);
			if("-1".equals(id))
				return;
			displayName=getUserName(rowe);
				
		}	
		catch(Exception e){			
		}

		String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		context.write(new Text(fileName.split("_")[0]+"_"+id), new Text(displayName));	   	
	}

	 String getUserName(Element e){
                String id = e.getAttribute("DisplayName");
                return id;
        }



	Element getRowElement(Text xmlValue) throws Exception{
		String row= xmlValue.toString();
                DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
                InputSource is = new InputSource();
                is.setCharacterStream(new StringReader(row));
                Document doc = db.parse(is);
                NodeList nl = doc.getElementsByTagName("row");
                Element e = (Element)nl.item(0);
		return e;

	}
    	
	 String getRowId(Element e) throws Exception {
                String rowId = e.getAttribute("Id");
                return rowId;
        }
  


}	



