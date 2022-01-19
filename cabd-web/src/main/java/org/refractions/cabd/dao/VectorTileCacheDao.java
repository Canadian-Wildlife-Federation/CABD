
/*
 * Copyright 2021 Canadian Wildlife Federation.
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
package org.refractions.cabd.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreatorFactory;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

/**
 * Data access interface for tile cache
 * 
 * @author Emily
 *
 */
@Component
public class VectorTileCacheDao {

	//cache table
	private static final String VECTOR_TILE_CACHE = "cabd.vector_tile_cache";
	//table columns
	private String TILECOL = "tile";
	private String LASTACCESSEDCOL = "last_accessed";
	private String KEYCOL = "key";
	
	@Autowired
	private JdbcTemplate jdbcTemplate;

	/**
	 * Gets the total size of the vector tile cache
	 * 
	 * @return
	 */
	public long getCacheSize() {
		// pg_relation_size('" + VECTOR_TILE_CACHE + "') 
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT sum(length(");
		sb.append(TILECOL);
		sb.append(")) / 1000 FROM ");
		sb.append(VECTOR_TILE_CACHE);
		
		List<Long> data = jdbcTemplate.query(sb.toString(),
				new RowMapper<Long>() {
					@Override
					public Long mapRow(ResultSet rs, int rowNum) throws SQLException {
						return rs.getLong(1);
					}
				});
		if (data.size() > 0)
			return data.get(0);
		return 0;
	}

	/**
	 * Delete all tiles in the cache up to the provided size, deleting oldest first.
	 * 
	 * @param sizeToDelete size to delete in mb
	 */
	public void cleanCache(long sizeToDelete) {

		long target = sizeToDelete * 1000;
		
		// list tiles by size and add up until the required total is reached
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT length(" + TILECOL +"), ");
		sb.append( LASTACCESSEDCOL );
		sb.append(" FROM ");
		sb.append(VECTOR_TILE_CACHE);
		sb.append(" ORDER BY ");
		sb.append(LASTACCESSEDCOL + " asc");

		// scroll the results
		PreparedStatementCreatorFactory psc = new PreparedStatementCreatorFactory(sb.toString());
		psc.setResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE);

		Timestamp toDelete = jdbcTemplate.query(psc.newPreparedStatementCreator((Object[]) null),
				new ResultSetExtractor<Timestamp>() {

					@Override
					public Timestamp extractData(ResultSet rs) throws SQLException, DataAccessException {
						Long cnt = 0l;
						while (rs.next()) {
							cnt += rs.getLong(1);
							if (cnt > target) {
								return rs.getTimestamp(2);
							}
						}
						rs.close();
						return null;
					}
				});

		// delete all tiles last accessed before this date
		if (toDelete != null) {
			String query = "DELETE FROM " + VECTOR_TILE_CACHE + " WHERE " + LASTACCESSEDCOL + " <= ? ";
			jdbcTemplate.update(query, new Object[] { toDelete }, new int[] { Types.TIMESTAMP });
		} else {
			String query = "TRUNCATE " + VECTOR_TILE_CACHE;
			jdbcTemplate.update(query);
		}

	}

	public byte[] getTile(String key) {

		StringBuilder sb = new StringBuilder();
		sb.append("UPDATE ");
		sb.append(VECTOR_TILE_CACHE);
		sb.append(" SET ");
		sb.append( LASTACCESSEDCOL + " = now() ");
		sb.append(" WHERE " );
		sb.append(KEYCOL + " = ? ");
		sb.append("RETURNING " + TILECOL);
		
		List<byte[]> data = jdbcTemplate.query(
				sb.toString(),
				new Object[] { key }, new int[] { Types.VARCHAR }, new RowMapper<byte[]>() {
					@Override
					public byte[] mapRow(ResultSet rs, int rowNum) throws SQLException {
						return rs.getBytes("tile");
					}
				});
		if (data.size() > 0)
			return data.get(0);
		return null;
	}

	public boolean containsKey(String key) {
		Integer cnt = jdbcTemplate.queryForObject("select count(*) from " + VECTOR_TILE_CACHE + " where " + KEYCOL + " = ?",
				new Object[] { key }, new int[] { Types.VARCHAR }, Integer.class);
		return cnt > 0;
	}

	public void setTile(String key, byte[] data) {
		jdbcTemplate.update("insert into " + VECTOR_TILE_CACHE + "(" + KEYCOL + "," + TILECOL + ") values (?,?)", key, data);
	}

	public void removeTile(String key) {
		jdbcTemplate.update("delete from " + VECTOR_TILE_CACHE + " set " + TILECOL + " = ? ", key);
	}

	public void clear() {
		jdbcTemplate.update("delete from " + VECTOR_TILE_CACHE);
	}
}
