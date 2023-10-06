/*
 * Copyright 2021 Canadian Wildlife Federation
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License.
 */
package org.refractions.cabd.controllers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;

import org.refractions.cabd.CabdConfigurationProperties;
import org.refractions.cabd.dao.DataSourceDao;
import org.refractions.cabd.dao.FeatureTypeManager;
import org.refractions.cabd.dao.UserFeatureUpdateDao;
import org.refractions.cabd.model.DataSource;
import org.refractions.cabd.model.FeatureType;
import org.refractions.cabd.model.FeatureTypeListValue;
import org.refractions.cabd.model.FeatureViewMetadataField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST api for Databaes Features
 * 
 * @author Emily
 *
 */
@RestController
@RequestMapping("/" + DocumentationController.PATH)
public class DocumentationController {

	public static final String PATH = "docs";
	
	@Autowired
	CabdConfigurationProperties properties;
	@Autowired
	DataSourceDao dataSourceDao;
	@Autowired
	UserFeatureUpdateDao featureUpdateDao;
	@Autowired
	FeatureTypeManager typeManager;
	
	/**
	 * Gets an individual feature by identifier
	 * 
	 * @param id
	 * @return
	 */
	@GetMapping(value = "", produces = MediaType.TEXT_HTML_VALUE)
	public String getDocumentation(HttpServletRequest request,
			@RequestParam(name="options", required=false) List<String> options) {
		
		StringBuilder sb = new StringBuilder();

		//document data sources
		if (hasOption(options, "ds")) {
			documentDataSources(sb);
		}
		
		//document feature types
		List<FeatureType> toAdd = new ArrayList<>();
		if (hasOption(options, "types")) {
			toAdd.addAll(typeManager.getFeatureTypes());
		}else {
			for (FeatureType ft : typeManager.getFeatureTypes()) {
				if (hasOption(options, ft.getType())) {
					toAdd.add(ft);
				}
			}
		}		
		if (!toAdd.isEmpty()) {
			documentFeatureTypes(sb, toAdd);
		}
		
		return sb.toString();
	}

	private void documentFeatureTypes(StringBuilder sb, List<FeatureType> toAdd) {
		//find all types 
		List<FeatureType> sortedTypes = toAdd;
		
		//remove "parent" types based on the feature source table being populated or not
		for (Iterator<FeatureType> iterator = sortedTypes.iterator(); iterator.hasNext();) {
			FeatureType featureType = (FeatureType) iterator.next();
			if (featureType.getFeatureSourceTable() == null) iterator.remove();
		}
		//sort by name
		sortedTypes.sort((a,b)->a.getName().compareTo(b.getName()));
		
		
		//create summary table
//		createHeader(sb,"Feature Types", null, 1);
		
		if (sortedTypes.size() > 1) {
			createTable(sb, "ft_datatable", null);
			
			createTableHeader(sb, "Name", "Version");
			boolean isodd = true;
			for (FeatureType ft : sortedTypes) {
				String name = "<a href=\"#ft_" + ft.getType() + "\">" + ft.getName() +"</a>";
				createTableRow(sb, isodd, name, ft.getDataVersion());
				isodd = !isodd;
			}
			endTable(sb);
		}
		
		//create section for each feature type
		for (FeatureType ft: sortedTypes) {
			documentFeatureType(sb, ft);
		}
		
		//add attributes
		for (FeatureType ft: sortedTypes) {
			createSection(sb, "ftatt_" + ft.getType());
			createHeader(sb, ft.getName() + " Attributes", null, 3);
			
			List<FeatureViewMetadataField> sorted = new ArrayList<>(ft.getViewMetadata().getFields());
			sorted.sort((a,b)->a.getName().compareTo(b.getName()));
			
			for (FeatureViewMetadataField attribute : sorted) {
				documentAttribute(sb, attribute, null, ft);				
			}
			endSection(sb);
		}
	}
	
	private void documentAttribute(StringBuilder sb, FeatureViewMetadataField attribute, String idPrefix, FeatureType ft) {
		sb.append("<div class=\"section\" id=\"" + (ft == null? idPrefix : ft.getType()) + "_" + attribute.getFieldName() + "\">");
				
		createHeader(sb, attribute.getName(), null, 4);
		
		sb.append("<blockquote>");
		sb.append("<div>");
		
		documentNameValuePair(sb, "Feature Type:", ft == null ? "All Feature Types" : ft.getName());
		documentNameValuePair(sb, "Definition:", attribute.getDescription());
		documentNameValuePair(sb, "Field Name:", attribute.getFieldName());
		documentNameValuePair(sb, "Data Type:", attribute.getDataType());
		if (attribute.getValidValuesReference() != null && refTableHasCode(attribute)) {
			createTable(sb, "datatable_attribute_" + attribute.getFieldName() + (ft == null ? "" : "_"+ft.getType()), "style=\"background-color: white\"");
			createTableHeader(sb, "Code", "Name", "Description");
			
			boolean isodd = true;
			for (FeatureTypeListValue v : attribute.getValueOptions()) {
				createTableRow(sb, isodd, v.getValue().toString(), v.getName(), v.getDescription());
				isodd = !isodd;
			}
			
			endTable(sb);
			
		}
		sb.append("</div>");
		sb.append("</blockquote>");
		
		sb.append("</div>");
		
	}
	
	private void documentDataSources(StringBuilder sb) {
//		createHeader(sb,"Data Sources", null, 1);
//		sb.append("<hr/>");
		
		createTable(sb, "ds_dataTable", null);
		createTableHeader(sb, "Data Type", "Source Type", "Organization", "Data Source Name", "Short Name");
		
		Map<String, Map<String, Map<String, List<DataSource>>>> sections = new HashMap<>();
		boolean isodd = true;
		for (DataSource ds : dataSourceDao.getDataSources()) {
			String link = "<a href=\"#ds_" + ds.getName() + "\">" + ds.getName() + "</a>";
			
			createTableRow(sb, isodd,
					ds.getType(),
					ds.getCategory(),
					ds.getOrganizationName(),
					ds.getFullName(),
					link);
			
			isodd = !isodd;
			
			String key1 = ds.getType();
			if (key1 == null || key1.isBlank()) key1 = "Other";
			
			String key2 = ds.getCategory();
			if (key2 == null || key2.isBlank()) key2 = "Other";
			
			String key3 = ds.getOrganizationName();
			if (key3 == null || key3.isBlank()) key3 = "Other";
			
			if (!sections.containsKey(key1)) {
				sections.put(key1, new HashMap<>());
			}
			if (!sections.get(key1).containsKey(key2)) {
				sections.get(key1).put(key2,  new HashMap<>());
			}
			if (!sections.get(key1).get(key2).containsKey(key3)) {
				sections.get(key1).get(key2).put(key3,  new ArrayList<>());
			}
			sections.get(key1).get(key2).get(key3).add(ds);
		}
		endTable(sb);
		
		List<String> sourceType = new ArrayList<>(sections.keySet());
		sourceType.sort((a,b)->{
			if (a.equals(b)) return 0;
			if (a.equals("spatial")) return -1;
			if (b.equals("spatial")) return 1;
			return a.compareTo(b);
		});
		
		for (String key1: sourceType) {
			
			String sname = "Other";
			if (key1.equals("spatial")) sname = "Spatial Data Sources";
			if (key1.equals("non-spatial")) sname = "Non-Spatial Data Sources";
			
			
			createSection(sb, "dsid_" + createKey(key1) );
			
			createHeader(sb, sname, null, 2);
			sb.append("<hr/>");
			
			List<String> categories = new ArrayList<>(sections.get(key1).keySet());
			categories.sort((a,b)->a.compareTo(b));
			for (String order : new String[] {"Municipal", "Provincial/State", "Provincial", "Federal"}) {					
				if (categories.remove(order)) categories.add(0, order);	
			}
			
			for (String key2: categories) {
				
				createSection(sb, "dsid_" + createKey(key1) + "_" + createKey(key2));
				createHeader(sb, key2, null, 3);
				
				List<String> organizations = new ArrayList<>(sections.get(key1).get(key2).keySet());
				organizations.sort((a,b)->a.compareTo(b));
				
				for (String key3: organizations) {
					createHeader(sb, key3, null, 4);
					
					List<DataSource> dss = sections.get(key1).get(key2).get(key3);
					dss.sort((a,b)->{
						String s1 = a.getFullName();
						if (s1 == null || s1.isEmpty()) s1 = a.getName();
						String s2 = b.getFullName();
						if (s2 == null || s2.isEmpty()) s2 = b.getName();
						return s1.compareTo(s2);
					});
					
					String lastName = null;
					for (DataSource ds : dss) {
						String name = ds.getFullName();
						if (name == null || name.isBlank()) name = ds.getName();
						if (lastName == null || !name.equals(lastName)) {
							createHeader(sb, name, "ds_" + ds.getName(), 5);
						}
						lastName = name;
						
						sb.append("<blockquote>");
						sb.append("<div>");
						sb.append("<p>");
						sb.append(transformLinks(ds.getSource()));
						sb.append("</p>");
						
						documentNameValuePair(sb, "Organization:", ds.getOrganizationName());
						documentNameValuePair(sb, "Licence:", transformLinks(ds.getLicense()));
						documentNameValuePair(sb, "Geographic coverage:", ds.getGeographicCoverage());
						documentNameValuePair(sb, "Source ID Field:", ds.getSourceFieldId());
						documentNameValuePair(sb, "Short Name:", ds.getName());
						documentNameValuePair(sb, "Data Type:", ds.getType());
						documentNameValuePair(sb, "Source Type:", ds.getCategory());
						
						
						sb.append("</div>");
						
						sb.append("</blockquote>");
					}
				}
				endSection(sb);
			}
			endSection(sb);
		}
	}
	
	private String createKey(String key) {
		return key.toLowerCase().replaceAll("[^a-zA-Z0-9]", "");
	}
	
	private void documentNameValuePair(StringBuilder sb, String name, String value) {
		if (value == null) return ;
		sb.append("<p>");
		sb.append("<strong>");
		sb.append(name);
		sb.append("</strong> ");
		sb.append(value);
		sb.append("</p>");
	}

	
	private void documentFeatureType(StringBuilder sb, FeatureType ft) {
		
		createSection(sb, "ft_" + ft.getType());
		createHeader(sb,ft.getName(), null, 3);
		
		sb.append("<dl class=\"simple\">");
		sb.append("<dt>");
		sb.append("Definition");
		sb.append("</dt>");
		sb.append("<dd>");
		sb.append("<p><em>");
		sb.append(ft.getDescription());
		sb.append("</em></p>");
		sb.append("</dd>");
		
		
		sb.append("<dt>");
		sb.append("Attributes");
		sb.append("</dt>");
		sb.append("<dd>");
		sb.append("<p>");
		
		List<FeatureViewMetadataField> fields = new ArrayList<>(ft.getViewMetadata().getFields());
		fields.sort((a,b)->a.getName().compareTo(b.getName()));
		for (FeatureViewMetadataField field : fields) {
			String ref = "<a href=\"#" + ft.getType() + "_" + field.getFieldName() + "\">" + field.getName() + "</a>";
			sb.append(ref);
			sb.append(", ");
		}
		sb.deleteCharAt(sb.length()-2);

		sb.append("</p>");
		sb.append("</dd>");
		sb.append("</dl>");
		endSection(sb);	
	}

	private boolean refTableHasCode(FeatureViewMetadataField ref) {
		return !ref.getValidValuesReference().split(";")[1].isBlank();
	}
	
	private void createHeader(StringBuilder sb, String header, String id, int level) {
		sb.append("<h");
		sb.append(level);
		if (id != null) sb.append(" id=\"" + id + "\"");
		sb.append(">");
		sb.append(header);
		sb.append("</h");
		sb.append(level);
		sb.append(">");
	}
		
	private void createSection(StringBuilder sb, String id) {
		sb.append("<section id=\"" + id + "\">");
	}
	private void endSection(StringBuilder sb) {
		sb.append("</section>");
	}
	private void createTable(StringBuilder sb, String id, String inner) {
		sb.append("<table class=\"table\"");
		if (id != null) sb.append(" id=\"" + id + "\"");
		if (inner != null) sb.append(inner);
		sb.append("/>");
	}
	
	private void endTable(StringBuilder sb) {
		sb.append("</table>");
	}
	
	private void createTableHeader(StringBuilder sb, String... headers ) {
		sb.append("<thead>");
		for (String x : headers) {
			sb.append("<th class=\"head\">");
			sb.append(x);
			sb.append("</th>");
		}
		sb.append("</thead>");
	}
	
	private void createTableRow(StringBuilder sb, boolean isodd, String... data ) {
		sb.append("<tr class=\"");
		if (isodd) {
			sb.append("row-odd odd");
		}else {
			sb.append("row-even even");
		}
		sb.append("\">");
		for (String x : data) {
			sb.append("<td>");
			sb.append(x == null ? "" : x);
			sb.append("</td>");
		}
		sb.append("</tr>");
	}
		
	private boolean hasOption(List<String> options, String option) {
		if (options == null || options.isEmpty()) return true;
		for (String o : options) {
			if (o.trim().equalsIgnoreCase(option)) return true;
		}
		return false;
	}
	
	//see: https://stackoverflow.com/questions/49425990/replace-url-in-text-with-href-in-java
	private String transformLinks(String text) {
		if (text == null) return null;
		
		String urlValidationRegex = "(https?|ftp)://(www\\d?|[a-zA-Z0-9]+)?.[a-zA-Z0-9-]+(\\:|.)([a-zA-Z0-9-.]+|(\\d+)?)([/?:].*)?";
		Pattern p = Pattern.compile(urlValidationRegex);
		Matcher m = p.matcher(text);
		StringBuffer sb = new StringBuffer();
		while (m.find()) {
			String found = m.group(0);
			found = Matcher.quoteReplacement(found);
			try {
				m.appendReplacement(sb, "<a href='" + found + "'>" + found + "</a>");
			}catch (Exception ex) {
				ex.printStackTrace();
			}
		}
		m.appendTail(sb);
		return sb.toString();
	}
}
