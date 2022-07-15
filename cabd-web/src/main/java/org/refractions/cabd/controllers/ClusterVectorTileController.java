/*
 * Copyright 2022 Canadian Wildlife Federation
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

import java.text.MessageFormat;

import org.locationtech.jts.geom.Envelope;
import org.refractions.cabd.dao.FeatureDao;
import org.refractions.cabd.dao.FeatureTypeManager;
import org.refractions.cabd.dao.VectorTileCacheDao;
import org.refractions.cabd.exceptions.NotFoundException;
import org.refractions.cabd.model.FeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.Cache.ValueWrapper;
import org.springframework.context.i18n.LocaleContextHolder;
import org.springframework.cache.CacheManager;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;


/**
 * Controller for providing barriers as clustered vector tiles
 * 
 * @author Emily
 *
 */
@RestController
@RequestMapping("/" + ClusterVectorTileController.PATH)
public class ClusterVectorTileController {
	
	public static final String PATH = "clustertile";
	
	static final Logger logger = LoggerFactory.getLogger(ClusterVectorTileController.class.getCanonicalName());
	
	@Autowired
	private FeatureTypeManager typeManager;
	
	@Autowired
	private FeatureDao featureDao;
	
	@Autowired
	private VectorTileCacheDao cacheDao;
	
	@Autowired
	private CacheManager cacheManager;
	
	//mvt media type
	public static MediaType MVT_MEDIATYPE = new MediaType("application", "vnd.mapbox-vector-tile");

	//tile format
	public static final String MVT_FORMAT = "mvt";
	
	//tile spec bounds and epsg code
	public static Envelope BOUNDS = new Envelope(-20037508.342789,20037508.342789,-20037508.342789,20037508.342789);
	public static int SRID = 3857;
	
	@RequestMapping(value = "/{type}/{z}/{x}/{y}.{format}", 
			method = {RequestMethod.GET},
			produces = "application/vnd.mapbox-vector-tile")
	public ResponseEntity<byte[]> getVectorTile(
			@PathVariable("type") String type,
			@PathVariable("z") int z,
			@PathVariable("x") int x,
			@PathVariable("y") int y,
			@PathVariable("format") String format) {
		
		FeatureType ftype = typeManager.getFeatureType(type);
		if (ftype == null) throw new NotFoundException(MessageFormat.format("The feature type ''{0}'' is not supported.", type));
		return getTile(z, x, y, format, ftype);
	}
	
	@RequestMapping(value = "/{z}/{x}/{y}.{format}", 
			method = {RequestMethod.GET},
			produces = "application/vnd.mapbox-vector-tile")
	public ResponseEntity<byte[]> getVectorTile(
			@PathVariable("z") int z,
			@PathVariable("x") int x,
			@PathVariable("y") int y,
			@PathVariable("format") String format) {
		return getTile(z, x, y, format, null);
	}
	
	public ResponseEntity<byte[]> getTile(
			int z, int x, int y,
			String format, 
			FeatureType ftype) {
		
		//1. ensure format is "mvt"
		if (!format.equalsIgnoreCase(MVT_FORMAT)) {
			//throw invalid format exception
			throw new IllegalArgumentException(MessageFormat.format("The tile format {0} is not supported.  Only {1} is supported", format, MVT_FORMAT));
		}
		
		//2. ensure the x,y is valid for the given z
		int numtiles = (int) Math.pow(2, z);
		if (x < 0 || x > numtiles || y < 0 || y > numtiles) {
			//throw invalid tile exception
			throw new IllegalArgumentException(MessageFormat.format("The tile ({0}, {1}) is not valid for zoom level {2}", x,y,z));
		}
		
		//3. build query to get features		
		byte[] tile = getTileInternal(z, x, y, ftype); 

		HttpHeaders headers = new HttpHeaders();
	    headers.setContentType(MVT_MEDIATYPE);
	    headers.setContentLength(tile.length);
		return new ResponseEntity<byte[]>(tile, headers, HttpStatus.OK);		
	}
	
	
	private byte[] getTileInternal(int z, int x, int y, FeatureType ftype) {
		String key = getTileKey(z, x, y, ftype);

		ValueWrapper v = cacheManager.getCache("vectortilecache").get(key);
		if (v != null && v.get() != null)
			return (byte[]) v.get();

		byte[] tile = cacheDao.getTile(key);
		if (tile == null || tile.length == 0) {
			tile = featureDao.getClusterVectorTile(z, x, y, ftype); 
			if (tile == null || tile.length == 0)
				return new byte[0];
		}
		
		cacheManager.getCache("vectortilecache").put(key, tile);
		return tile;
	}

	private String getTileKey(int z, int x, int y, FeatureType layer) {
		String ft = "all";
		if (layer != null) ft = layer.getType();
		return "c_" + ft + "_" + z + "_" + x + "_" + y + "_" + LocaleContextHolder.getLocale().getLanguage().toLowerCase();
	}
	
}
