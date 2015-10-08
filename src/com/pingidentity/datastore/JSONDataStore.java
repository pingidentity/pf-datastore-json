package com.pingidentity.datastore;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import org.sourceid.saml20.adapter.attribute.AttributeValue;
import org.sourceid.saml20.adapter.conf.Configuration;
import org.sourceid.saml20.adapter.conf.SimpleFieldList;
import org.sourceid.saml20.adapter.gui.AdapterConfigurationGuiDescriptor;
import org.sourceid.saml20.adapter.gui.CheckBoxFieldDescriptor;
import org.sourceid.saml20.adapter.gui.RadioGroupFieldDescriptor;
import org.sourceid.saml20.adapter.gui.TextFieldDescriptor;
import org.sourceid.saml20.adapter.gui.validation.impl.IntegerValidator;

import com.pingidentity.sources.CustomDataSourceDriver;
import com.pingidentity.sources.CustomDataSourceDriverDescriptor;
import com.pingidentity.sources.SourceDescriptor;
import com.pingidentity.sources.gui.FilterFieldsGuiDescriptor;

// retrieve JSON from URL / file
// if URL - mins to cache?
// if file - location / filename (browse)


public class JSONDataStore implements CustomDataSourceDriver
{
	private static final String DATA_SOURCE_NAME = "JSON Data Store";
	private static final String DATA_SOURCE_CONFIG_DESC = "Configuration settings for the JSON data store";

    private static final String CONFIG_JSON_SOURCE_FILE_FILENAME_NAME = "JSON Filename";
	private static final String CONFIG_JSON_SOURCE_FILE_FILENAME_DESC = "(filename) The file name of the JSON file located in the \"conf\" directory.";   
        
    private static final String CONFIG_JSON_ID_ATTRIBUTE_NAME = "ID Attribute";
    private static final String CONFIG_JSON_ID_ATTRIBUTE_DESC = "Attribute to compare to the value provided in the filter.";
                                 
    private static final String CONFIG_JSON_ID_CASE_SENSITIVE_NAME = "Case Sensitive?";
    private static final String CONFIG_JSON_ID_CASE_SENSITIVE_DESC = "When comparing the ID attribute, should the test be case-sensitive.";

    private static final String CONFIG_FILTER_NAME = "Datastore filter";
    private static final String CONFIG_FILTER_DESC = "Value to compare to ID value.  (ie ${Username} for the username from the HTML adapter).";
    
    private Log log = LogFactory.getLog(this.getClass());
    private final CustomDataSourceDriverDescriptor descriptor;

    private String jsonFileName;
    private String idAttribute;
    private Boolean caseSensitiveSearch;
    
    private JSONParser parser = new JSONParser();
    

    public JSONDataStore()
    {
        // create the configuration descriptor for our custom data store
        AdapterConfigurationGuiDescriptor dataStoreConfigGuiDesc = new AdapterConfigurationGuiDescriptor(DATA_SOURCE_CONFIG_DESC);

		TextFieldDescriptor sourceFileName = new TextFieldDescriptor(CONFIG_JSON_SOURCE_FILE_FILENAME_NAME, CONFIG_JSON_SOURCE_FILE_FILENAME_DESC);
        dataStoreConfigGuiDesc.addField(sourceFileName);

		TextFieldDescriptor idAttribute = new TextFieldDescriptor(CONFIG_JSON_ID_ATTRIBUTE_NAME, CONFIG_JSON_ID_ATTRIBUTE_DESC);
        dataStoreConfigGuiDesc.addField(idAttribute);

        CheckBoxFieldDescriptor idCaseSensitivity = new CheckBoxFieldDescriptor(CONFIG_JSON_ID_CASE_SENSITIVE_NAME, CONFIG_JSON_ID_CASE_SENSITIVE_DESC);
		idCaseSensitivity.setDefaultValue(false);
        dataStoreConfigGuiDesc.addAdvancedField(idCaseSensitivity);
       
        // Add the configuration field for the search Filter
        FilterFieldsGuiDescriptor filterFieldsDescriptor = new FilterFieldsGuiDescriptor();
        filterFieldsDescriptor.addField(new TextFieldDescriptor(CONFIG_FILTER_NAME, CONFIG_FILTER_DESC));
        
        descriptor = new CustomDataSourceDriverDescriptor(this, DATA_SOURCE_NAME, dataStoreConfigGuiDesc, filterFieldsDescriptor);
    }

    @Override
    public SourceDescriptor getSourceDescriptor()
    {
        return descriptor;
    }

	@Override
    public void configure(Configuration configuration)
    {
    	log.debug("---[ Configuring JSON Data Store ]------");
   		jsonFileName = configuration.getFieldValue(CONFIG_JSON_SOURCE_FILE_FILENAME_NAME);
    	idAttribute = configuration.getFieldValue(CONFIG_JSON_ID_ATTRIBUTE_NAME);
    	caseSensitiveSearch = configuration.getBooleanFieldValue(CONFIG_JSON_ID_CASE_SENSITIVE_NAME);
    	log.debug("---[ Configuration complete ]------");
    }

    @Override
    public boolean testConnection()
    {
    	log.debug("---[ Testing connectivity to JSON Data Store ]------");
    	String fullFileName = System.getProperty("pf.server.default.dir") + "/conf/" + jsonFileName;
    	log.debug("Checking file: " + fullFileName);

    	try
        {
            File jsonDataFileHandle = new File(fullFileName);
            return jsonDataFileHandle.canRead() && jsonDataFileHandle.isFile();
        }
        catch (Exception e)
        {
            // do nothing
        }

        return false;
    }

    @Override
    public Map<String, Object> retrieveValues(Collection<String> attributeNamesToFill, SimpleFieldList filterConfiguration)
    {
    	log.debug("---[ Retrieving Values ]------");

		String idAttributeValue = filterConfiguration.getFieldValue(CONFIG_FILTER_NAME);

    	JSONObject jsonFileContents = null;
    	
    	try {
        	jsonFileContents = parseJSONFile();

			// grab the "users" array from the JSON file
			JSONArray usersArray = (JSONArray)jsonFileContents.get("users");
			
        	if (!usersArray.isEmpty()) {
        		
        		log.debug("--- looking for " + idAttribute + " == " + idAttributeValue);

        		for(Object currentObject : usersArray) {
        			
        			JSONObject thisElement = (JSONObject)currentObject;
        			
        			if(thisElement.containsKey(idAttribute)) {
        				
        				String valueToCheck = (String)thisElement.get(idAttribute);
        				
        				if(valueToCheck.equals(idAttributeValue)) {
        	        		log.debug("--- found record for " + idAttributeValue);
        					return attributesFromMap(thisElement, attributeNamesToFill);
        				}
        			}
        		}
        	}

    	} catch(IOException ex) {
    		
    	}

		log.debug("--- did NOT find record for " + idAttributeValue);
    	Map<String, Object> returnMap = new HashMap<String, Object>();
    	
		for(String attribute : attributeNamesToFill) {
			returnMap.put(attribute, "null");
		}

		return returnMap;
    }

	@SuppressWarnings("unchecked")
	@Override
    public List<String> getAvailableFields()
    {
    	log.debug("---[ Retrieving Available Fields ]------");
    	JSONObject jsonFileContents = null;
    	
    	try {
        	jsonFileContents = parseJSONFile();

			// grab the "users" array from the JSON file
			JSONArray usersArray = (JSONArray)jsonFileContents.get("users");
			
        	if (!usersArray.isEmpty()) {
        		JSONObject firstElement = (JSONObject)usersArray.get(0);

        		return new ArrayList<String>(sortList(firstElement.keySet()));
        	}

    	} catch(IOException ex) {
    		
    	}
    	
    	return null;
    }

    
	private Map<String, Object> attributesFromMap(JSONObject jsonValue, Collection<String> attributeNamesToFill)
	{
		Map<String, Object> returnMap = new HashMap<String, Object>();
		
		for(String attribute : attributeNamesToFill) {
			
			if (jsonValue != null && jsonValue.containsKey(attribute)) {
				Object jsonAttribute = jsonValue.get(attribute);
				AttributeValue attrToAdd = null;

				if (jsonAttribute instanceof JSONArray) {
					attrToAdd = new AttributeValue((List<String>)jsonAttribute);
				} else {
					attrToAdd = new AttributeValue((String)jsonAttribute);
				}
				
        		log.debug("--- adding " + attribute + " -> " + attrToAdd);
				returnMap.put(attribute, attrToAdd);
			} else {
				returnMap.put(attribute, null);
			}
		}
		
		return returnMap;
	}

    private synchronized JSONObject parseJSONFile() throws IOException
    {
    	String fullFileName = System.getProperty("pf.server.default.dir") + "/conf/" + jsonFileName;
    	
		BufferedReader reader = new BufferedReader(new FileReader(fullFileName));
		Object fileContents = null;
		
		try
        {
        	fileContents = parser.parse(reader);
        } catch (Exception ex) {
        	log.debug("Error parsing file - " + ex.getMessage());
        } finally {
        	reader.close();
        }
		
		return (JSONObject)fileContents;
    }

    public static <T extends Comparable<? super T>> List<T> sortList(Collection<T> c) {
      List<T> list = new ArrayList<T>(c);
      java.util.Collections.sort(list);
      return list;
    }
}
