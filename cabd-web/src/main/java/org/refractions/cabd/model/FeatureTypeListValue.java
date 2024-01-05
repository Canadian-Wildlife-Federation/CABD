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
package org.refractions.cabd.model;

/**
 * Valid list item for code/list attributes
 * 
 * @author Emily
 *
 */
public class FeatureTypeListValue extends NamedDescriptionItem implements Comparable<FeatureTypeListValue>{

	private Object value;
	
	private double[] bbox;
	
	public FeatureTypeListValue(Object value, String name_en, 
			String name_fr, String description_en, String description_fr) {
		super(name_en, name_fr, description_en, description_fr);
		this.bbox = null;
		this.value = value;
	}
	
	public FeatureTypeListValue(Object value, String name_en, 
			String name_fr, String description_en, String description_fr,
			Double minx, Double miny, Double maxx, Double maxy) {
		super(name_en, name_fr, description_en, description_fr);
		this.bbox = new double[]{minx, miny, maxx, maxy};
		this.value = value;
	}
	
	public double[] getBbox() {
		return bbox;
	}
	
	public Object getValue() {
		if (this.value == null) return getName();
		return this.value;
	}

	@Override
	public int compareTo(FeatureTypeListValue o) {
		if (value instanceof Comparable ) return ((Comparable) value).compareTo(o.getValue());
		return 0;
	}
	
}
