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

public class PostsParserMapper extends Mapper<LongWritable, Text, Text, Text> {

	private static final Log LOG = LogFactory.getLog(PostsParserMapper.class);

	private static final Pattern wlPattern = Pattern.compile("\\[\\[.+?\\]\\]");

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String postTypeId = "";
		String id = "";
		String tags = null;
		String userid = "";
		try {
			Element rowe = getRowElement(value);
			postTypeId = getPostTypeId(rowe);
			userid = getUserId(rowe);
			if (userid == null || userid.isEmpty())
				userid = getUserName(rowe);
			if ("1".equals(postTypeId)) {// question
				id = getRowId(rowe);
				tags = getTags(rowe);
			} else if ("2".equals(postTypeId)) {
				id = getParentId(rowe);

			}

		} catch (Exception e) {
		}
		if (tags == null)
			tags = "<NULL_830750047>";

		String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		if (userid == null || userid.isEmpty() || "-1".equals(userid) || id == null || id.isEmpty() || "-1".equals(id))
			return;

		else {
			System.out.println("userid-" + userid);
			context.write(new Text(fileName + "_" + id),
					new Text(fileName.split("_")[0] + "_" + userid + "<830750047>" + tags));
		}
	}

	String getUserName(Element e) {
		String id = e.getAttribute("OwnerDisplayName");
		return id;
	}

	String getUserId(Element e) {
		String id = e.getAttribute("OwnerUserId");
		return id;
	}

	String getParentId(Element e) {
		String id = e.getAttribute("ParentId");
		return id;
	}

	String getTags(Element e) {
		String tagsString = e.getAttribute("Tags");
		return tagsString;
	}

	Element getRowElement(Text xmlValue) throws Exception {
		String row = xmlValue.toString();
		DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
		InputSource is = new InputSource();
		is.setCharacterStream(new StringReader(row));
		Document doc = db.parse(is);
		NodeList nl = doc.getElementsByTagName("row");
		Element e = (Element) nl.item(0);

		return e;

	}

	String getPostTypeId(Element e) throws Exception {
		String postTypeId = e.getAttribute("PostTypeId");
		return postTypeId;
	}

	String getRowId(Element e) throws Exception {
		String rowId = e.getAttribute("Id");
		return rowId;
	}

}
