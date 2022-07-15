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

import java.io.IOException;
import java.io.OutputStream;
import java.sql.Date;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map.Entry;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.postgresql.jdbc.PgArray;
import org.refractions.cabd.dao.FeatureDao;
import org.refractions.cabd.model.Feature;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

/**
 * Utilties for writing geometry objects to GeoJson. 
 * 
 * @author Emily
 *
 */
public enum GeoJsonUtils {

	INSTANCE;
	
	/*
	 * 
	 * This code does not use a json library - originally used jackson library
	 * but found it very slow for large result sets.
	 */
	/**
	 * Converts a feature to geo-json streaming results to output stream
	 * 
	 * 
	 * @param feature
	 * @param stream
	 * @throws IOException
	 */
	public void writeFeature(Feature feature, OutputStream stream) throws IOException {

		writeString(stream, "{" + convertKey("type") + convertObject("Feature") + "," );
		// geometry
		if (feature.getGeometry() != null) writeGeometry(feature.getGeometry(), stream);

		// properties
		writeString(stream, ", " + convertKey("properties") + "{");
		writeString(stream, convertKey(FeatureDao.ID_FIELD) + convertObject(feature.getId().toString()) + ",");
		
		try {
			boolean first = true;
			//attributes
			for (Entry<String, Object> prop : feature.getAttributes().entrySet()) {
				
				if (!first) {
					writeString(stream, ",");	
				}
				first = false;
				
				Object propValue = prop.getValue();
				if (propValue instanceof PgArray) {
					StringBuilder sb = new StringBuilder();
					sb.append(convertKey(prop.getKey()));
					sb.append("[ ");
					for (Object item : (Object[])((PgArray)propValue).getArray() ) {
						sb.append(convertObject(item));
						sb.append(",");
					}
					sb.deleteCharAt(sb.length() - 1);
					sb.append("]");
					writeString(stream, sb.toString());	
				}else {
					writeString(stream, convertKey(prop.getKey()) + convertObject(propValue) );
				}
			}
			
			//links
			String rooturl = ServletUriComponentsBuilder.fromCurrentContextPath().path("/").build().toUriString();
			StringBuilder sb = new StringBuilder();
			for (Entry<String, String> link : feature.getLinkAttributes().entrySet()) {
				if (!first) {
					sb.append(",");	
				}
				first = false;
				sb.append(convertKey(link.getKey()));
				if (link.getValue() != null) {
					sb.append( convertObject(rooturl + link.getValue() ) );
				}else {
					sb.append("null");
				}

			}
			writeString(stream, sb.toString());
		}catch (SQLException ex) {
			throw new IOException(ex);
		}
		writeString(stream, "}}");
		
	}

	private String convertKey(String key) {
		return "\"" + key + "\":";
	}
	
	private String convertObject(Object value) {
		if (value == null) return "null";
		if (value instanceof String) return "\"" + escape(value.toString()) + "\"";
		if (value instanceof Number) return String.valueOf( ((Number)value) );
		if (value instanceof Boolean) {
			if ((Boolean)value) return "true";
			return "false";
		}
		if (value instanceof LocalDate) {
			return "\"" + DateTimeFormatter.ISO_DATE.format( (LocalDate)value ) + "\"";
		}
		if (value instanceof Date) {
			return "\"" + DateTimeFormatter.ISO_DATE.format( ((Date)value).toLocalDate() ) + "\"";
		}
		return "\"" + escape(value.toString()) + "\""; 		
	}
	
	private void writeString(OutputStream stream, String value) throws IOException {
		stream.write(value.getBytes());
	}
	
	private void writeGeometry(Geometry g, OutputStream stream) throws IOException {

		writeString(stream, convertKey("geometry") 
				+ "{" + convertKey("type") 
				+ convertObject(g.getGeometryType()) + ","
				+ convertKey("coordinates")
				);

		switch (g.getGeometryType()) {
		case "Point":
			writeString(stream, coordinate(((Point) g).getX(), ((Point) g).getY()));
			break;
		case "MultiPoint":
			writeString(stream, coordinates(((MultiPoint) g).getCoordinates()));
			break;
		case "LineString":
			writeString(stream, coordinates(((LineString) g).getCoordinateSequence()));
			break;
		case "Polygon":
			writeString(stream, polygon(((Polygon) g)));
			break;
		case "MultiPolygon":
			writeString(stream, multiPolygon(((MultiPolygon) g)));
			break;
		default:
			writeString(stream, "\"Unknown geometry type \"");
		}
		writeString(stream, "}");
	}

	private String multiPolygon(MultiPolygon mp) throws IOException {
		StringBuilder sb = new StringBuilder();
		sb.append("[");
		
		for (int i = 0; i < mp.getNumGeometries(); i++) {
			if (i != 0) sb.append(",");
			sb.append(polygon((Polygon) mp.getGeometryN(i)));
		}
		sb.append("]");
		return sb.toString();
	}

	private String polygon(Polygon p) throws IOException {
		StringBuilder sb = new StringBuilder();
		sb.append("[");
		sb.append(coordinates(p.getExteriorRing().getCoordinateSequence()));
		for (int i = 0; i < p.getNumInteriorRing(); i++) {
			if (i != 0) sb.append(",");
			sb.append(coordinates(p.getInteriorRingN(i).getCoordinateSequence()));
		}
		sb.append("]");
		return sb.toString();
	}

	private String coordinates(CoordinateSequence cs) throws IOException {
		
		StringBuilder sb = new StringBuilder();
		sb.append("[");
		for (int i = 0; i < cs.size(); i++) {
			if (i != 0) sb.append(",");
			sb.append(coordinate(cs.getX(i), cs.getY(i)));
		}
		sb.append("]");
		return sb.toString();
	}

	private String coordinates(Coordinate[] cs) throws IOException {
		StringBuilder sb = new StringBuilder();
		sb.append("[");
		for (int i = 0; i < cs.length; i++) {
			if (i != 0) sb.append(",");
			sb.append(coordinate(cs[i].x, cs[i].y));
		}
		sb.append("]");
		return sb.toString();
	}
	
	private String coordinate(double x, double y) throws IOException {
		return "[" + x + ", " + y + "]";
	}
	
	
	
	
	/* copied from simple-json library*/
	/**
	 * Escape quotes, \, /, \r, \n, \b, \f, \t and other control characters (U+0000 through U+001F).
	 * @param s
	 * @return
	 */
	public static String escape(String s){
		if(s==null)
			return null;
        StringBuffer sb = new StringBuffer();
        escape(s, sb);
        return sb.toString();
    }

    /**
     * @param s - Must not be null.
     * @param sb
     */
    static void escape(String s, StringBuffer sb) {
    	final int len = s.length();
		for(int i=0;i<len;i++){
			char ch=s.charAt(i);
			switch(ch){
			case '"':
				sb.append("\\\"");
				break;
			case '\\':
				sb.append("\\\\");
				break;
			case '\b':
				sb.append("\\b");
				break;
			case '\f':
				sb.append("\\f");
				break;
			case '\n':
				sb.append("\\n");
				break;
			case '\r':
				sb.append("\\r");
				break;
			case '\t':
				sb.append("\\t");
				break;
			case '/':
				sb.append("\\/");
				break;
			default:
                //Reference: http://www.unicode.org/versions/Unicode5.1.0/
				if((ch>='\u0000' && ch<='\u001F') || (ch>='\u007F' && ch<='\u009F') || (ch>='\u2000' && ch<='\u20FF')){
					String ss=Integer.toHexString(ch);
					sb.append("\\u");
					for(int k=0;k<4-ss.length();k++){
						sb.append('0');
					}
					sb.append(ss.toUpperCase());
				}
				else{
					sb.append(ch);
				}
			}
		}//for
	}
}
