package com.lchen.da.udf;

import java.io.IOException;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/** 
 * @author hzchenlei1
 *
 * 2018-1-11
 */
public class FromJsonUDF extends GenericUDF {
	private StringObjectInspector jsonInspector;
	private InspectorHandle inspHandle;
	
	
	@Override
	public Object evaluate(DeferredObject[] arg0) throws HiveException {
	try {
	    String jsonString = jsonInspector.getPrimitiveJavaObject(arg0[0].get());
	    if (jsonString == null)
	        return null;
	
	    ObjectMapper jacksonParser = new ObjectMapper();
	    JsonNode jsonNode = jacksonParser.readTree(jsonString);
	
	    return inspHandle.parseJson(jsonNode);
	} catch (JsonProcessingException e) {
	    throw new HiveException(e);
	} catch (IOException e) {
	    throw new HiveException(e);
	}
	}
	
	@Override
	public String getDisplayString(String[] arg0) {
	return "from_json( \"" + arg0[0] + "\" , \"" + arg0[1] + "\" )";
	}
	
	@Override
	public ObjectInspector initialize(ObjectInspector[] arg0)
	    throws UDFArgumentException {
	if (arg0.length != 2) {
	    throw new UDFArgumentException("from_json expects a JSON string and a template object");
	}
	if (arg0[0].getCategory() != Category.PRIMITIVE
	        || ((PrimitiveObjectInspector) arg0[0]).getPrimitiveCategory() != PrimitiveCategory.STRING) {
	    throw new UDFArgumentException("from_json expects a JSON string and a template object");
	}
	jsonInspector = (StringObjectInspector) arg0[0];
	if (arg0[1].getCategory() == Category.PRIMITIVE
	        && ((PrimitiveObjectInspector) arg0[1]).getPrimitiveCategory() == PrimitiveCategory.STRING) {
	    if (!(arg0[1] instanceof ConstantObjectInspector)) {
	        throw new UDFArgumentException("typeinfo string must be constant");
	    }
	    ConstantObjectInspector typeInsp = (ConstantObjectInspector) arg0[1];
	
	    String typeStr = typeInsp.getWritableConstantValue().toString();
	    inspHandle = InspectorHandle.InspectorHandleFactory.GenerateInspectorHandleFromTypeInfo(typeStr);
	} else {
	    inspHandle = InspectorHandle.InspectorHandleFactory.GenerateInspectorHandle(arg0[1]);
	}
	
	return inspHandle.getReturnType();
	}
	
	
	static public String ToCamelCase(String underscore) {
	StringBuilder sb = new StringBuilder();
	String[] splArr = underscore.toLowerCase().split("_");
	
	sb.append(splArr[0]);
	for (int i = 1; i < splArr.length; ++i) {
	    String word = splArr[i];
	    char firstChar = word.charAt(0);
	    if (firstChar >= 'a' && firstChar <= 'z') {
	        sb.append((char) (word.charAt(0) + 'A' - 'a'));
	        sb.append(word.substring(1));
	    } else {
	        sb.append(word);
	    }
	
	}
	return sb.toString();
	}
	
	/**
	* Converts from CamelCase to a string containing
	* underscores.
	*
	* @param camel
	* @return
	*/
	static public String FromCamelCase(String camel) {
	StringBuilder sb = new StringBuilder();
	for (int i = 0; i < camel.length(); ++i) {
	    char ch = camel.charAt(i);
	    if (ch >= 'A' && ch <= 'Z') {
	        sb.append('_');
	        sb.append((char) (ch - 'A' + 'a'));
	    } else {
	        sb.append(ch);
	    }
	}
	return sb.toString();
	}

}




