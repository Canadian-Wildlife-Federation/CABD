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
import java.text.MessageFormat;
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
public class Filter {

	//list of query parts
	private List<FilterPart> filters;
	
	public Filter(List<FilterPart> parts) {
		this.filters = parts;
	}
	
	public List<FilterPart> getFilters(){
		return this.filters;
	}
	
	public Object[] toSql(FeatureViewMetadata metadata) {
	
		validate(metadata);
		
		List<Object> parameters = new ArrayList<>();
		StringBuilder sb = new StringBuilder();
		boolean and = false;
		for (FilterPart e : filters) {
			if (and) sb.append(" AND ");	
			and = true;
			
			if (e.getDataType() == DataType.STRING &&
					(e.getOperator() != Operator.NULL && e.getOperator() != Operator.NOTNULL)) {
				sb.append("LOWER(");
				sb.append(e.getAttributeKey());
				sb.append(")");
			}else {
				sb.append(e.getAttributeKey());
			}
			sb.append(" " );
			sb.append(e.getOperator().toSql());
			sb.append(" " );
			
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
		
		return new Object[] {sb.toString(), parameters};
	}
	
	/*
	 * validates the filter
	 */
	private boolean validate(FeatureViewMetadata metadata) {

		//ensure all attributes are valid
		for (FilterPart p : filters) {
			FeatureViewMetadataField field = null; 
			for (FeatureViewMetadataField f : metadata.getFields()) {
				if (f.getFieldName().equalsIgnoreCase(p.getAttributeKey())) {
					field = f;
					break;
				}	
			}
			if (field == null) {
				throw new InvalidParameterException(MessageFormat.format("Feature view {0} does not contain attribute {1}.", metadata.getFeatureView(), p.getAttributeKey()));
			}
			
			//validate operator based on attribute type
			DataType dataType = DataType.findDataType(field.getDataType());
			if (!p.getOperator().supports(dataType)) {
				throw new InvalidParameterException(MessageFormat.format("Attribute {0} of type {1} doesn''t support operator {2}", p.getAttributeKey(), field.getDataType(), p.getOperator().key));
			}
			p.setDataType(dataType);
		}
		return true;
	}
	
	/**
	 * Parse a set of filters 
	 * 
	 * @param bits
	 * @return
	 */
	public static Filter parseFilter(String[] bits) {
		List<FilterPart> parts = new ArrayList<>();
		for (String bit : bits) {
			parts.add(parseFilterPart(bit));
		}
		return new Filter(parts);
	}
	
	/**
	 * Parse an individual filter type.
	 * 
	 * @param filter
	 * @return
	 */
	private static FilterPart parseFilterPart(String filter) {
		
		int first = filter.indexOf(':');
		int second = filter.indexOf(':',  first + 1);
		
		if (first == -1 || second == -1) {
			throw new InvalidParameterException("Invalid filter string: " + filter);
		}
		
		String attributeKey = filter.substring(0, first);
		Operator op = Operator.findOperator(filter.substring(first + 1, second));
		String[] values = null;
		if (op == Operator.IN || op == Operator.NOTIN) {
			values = filter.substring(second+1).split(",");
		}else {
			values = new String[] {filter.substring(second + 1)};
		}
		
		return new FilterPart(attributeKey, op, values);
	}
	
}
