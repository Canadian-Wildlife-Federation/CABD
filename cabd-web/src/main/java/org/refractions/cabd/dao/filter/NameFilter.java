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
package org.refractions.cabd.dao.filter;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;

import org.refractions.cabd.model.FeatureViewMetadata;
import org.refractions.cabd.model.FeatureViewMetadataField;

/**
 * Represent a query filter
 * 
 * @author Emily
 *
 */
public class NameFilter {

	//list of query parts
	private List<FilterPart> filters;
	
	public NameFilter(List<FilterPart> parts) {
		this.filters = parts;
	}
	
	public List<FilterPart> getFilters(){
		return this.filters;
	}
	
	public Object[] toSql(FeatureViewMetadata metadata) {
	
		for (FilterPart p : filters) p.setDataType(DataType.STRING);

		List<String> nameFields = new ArrayList<>();
		for (FeatureViewMetadataField field : metadata.getFields()) {
			if (field.isNameSearch()) nameFields.add(field.getFieldName());
		}
		if (nameFields.isEmpty()) {
			//TODO: nothing to search so what should we do???
			return null;
		}
		
		List<Object> parameters = new ArrayList<>();
		StringBuilder sb = new StringBuilder();
		boolean and = false;
		for (FilterPart e : filters) {
			if (and) sb.append(" OR ");	
			and = true;
			
			sb.append("(");
			boolean or = false;
			for (String s : nameFields) {
				if (or) sb.append(" OR ");
				or = true;
				
				sb.append("LOWER(");
				sb.append(s);
				sb.append(") ");
				sb.append(e.getOperator().toSql());
				sb.append(" ");
				
				if (e.getOperator() == Operator.IN || e.getOperator() == Operator.NOTIN) {
					sb.append("(");
					for (Object value : e.getValuesAsDataType()) {
						if (e.getDataType() == DataType.STRING) {
							parameters.add(value.toString().toLowerCase());
						}else {
							parameters.add(value);
						}
						sb.append("?,");
					}
					sb.deleteCharAt(sb.length() - 1);
					sb.append(")");
				}else {
					if (e.getOperator() != Operator.NULL && e.getOperator() != Operator.NOTNULL) {
						Object value = e.getValuesAsDataType()[0];
						if (e.getOperator() == Operator.LIKE) {
							String str = value.toString();
							str = str.replaceAll("_", "\\\\_").replaceAll("%", "\\\\%");
							value = "%" + str + "%";
						}
						sb.append("?");
						if (e.getDataType() == DataType.STRING) {
							parameters.add(value.toString().toLowerCase());
						}else {
							parameters.add(value);
						}
					}
				}
			}
			sb.append(")");
		}
		
		return new Object[] {sb.toString(), parameters};
	}
	

	
	/**
	 * Parse a set of filters 
	 * 
	 * @param bits
	 * @return
	 */
	public static NameFilter parseFilter(String[] bits) {
		List<FilterPart> parts = new ArrayList<>();
		for (String bit : bits) {
			parts.add(parseFilterPart(bit));
		}
		return new NameFilter(parts);
	}
	
	/**
	 * Parse an individual filter type.
	 * 
	 * @param filter
	 * @return
	 */
	private static FilterPart parseFilterPart(String filter) {
		
		int first = filter.indexOf(':');
		
		if (first == -1 ) {
			throw new InvalidParameterException("Invalid filter string: " + filter);
		}
		
		String attributeKey = "";
		Operator op = Operator.findOperator(filter.substring(0, first));
		String[] values = null;
		if (op == Operator.IN || op == Operator.NOTIN) {
			values = filter.substring(first + 1).split(",");
		}else {
			values = new String[] {filter.substring(first + 1)};
		}
		
		return new FilterPart(attributeKey, op, values);
	}
	
}
