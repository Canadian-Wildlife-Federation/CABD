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

import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import com.fasterxml.jackson.core.JsonGenerator;

/**
 * Utilties for writing geometry objects to GeoJson. 
 * 
 * @author Emily
 *
 */
public enum GeoJsonUtils {

	INSTANCE;
	
	/**
	 * Writes the geometry object a geometry
	 * field in the Json Generator
	 * @param gen
	 * @param g
	 * @throws IOException
	 */
	public void writeGeometry(JsonGenerator gen, Geometry g) throws IOException {

		gen.writeObjectFieldStart("geometry");

		gen.writeStringField("type", g.getGeometryType());

		gen.writeFieldName("coordinates");
		switch (g.getGeometryType()) {
		case "Point":
			coordinate(gen, ((Point) g).getX(), ((Point) g).getY());
			break;
		case "LineString":
			coordinates(gen, ((LineString) g).getCoordinateSequence());
			break;
		case "Polygon":
			polygon(gen, ((Polygon) g));
			break;
		case "MultiPolygon":
			multiPolygon(gen, ((MultiPolygon) g));
			break;
		default:
			gen.writeString("Unknown geometry type");
		}

		gen.writeEndObject();

	}

	private void multiPolygon(JsonGenerator gen, MultiPolygon mp) throws IOException {
		gen.writeStartArray();
		for (int i = 0; i < mp.getNumGeometries(); i++) {
			polygon(gen, (Polygon) mp.getGeometryN(i));
		}
		gen.writeEndArray();
	}

	private void polygon(JsonGenerator gen, Polygon p) throws IOException {
		gen.writeStartArray();
		coordinates(gen, p.getExteriorRing().getCoordinateSequence());
		for (int i = 0; i < p.getNumInteriorRing(); i++) {
			coordinates(gen, p.getInteriorRingN(i).getCoordinateSequence());
		}
		gen.writeEndArray();
	}

	private void coordinates(JsonGenerator gen, CoordinateSequence cs) throws IOException {
		gen.writeStartArray();
		for (int i = 0; i < cs.size(); i++) {
			coordinate(gen, cs.getX(i), cs.getY(i));
		}
		gen.writeEndArray();
	}

	private void coordinate(JsonGenerator gen, double x, double y) throws IOException {
		gen.writeStartArray();
		gen.writeNumber(x);
		gen.writeNumber(y);
		gen.writeEndArray();
	}
}
