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
package org.refractions.cabd.dao;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

import org.refractions.cabd.model.DataSource;
import org.refractions.cabd.model.FeatureType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.SqlTypeValue;
import org.springframework.stereotype.Component;

/**
 * Manager features 
 * 
 * @author Emily
 *
 */
@Component
public class DataSourceDao {

	@Autowired
	private JdbcTemplate jdbcTemplate;
	
	/**
	 * List of datasources 
	 * 
	 * @return list of DataSources
	 */
	public List<DataSource> getDataSources(){
		
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT name, source_type, version_date, version_number, ");
		sb.append("data_source_category, organization_name, geographic_coverage, source_id_field, license, full_name, source");
		sb.append(" FROM ");
		sb.append(" cabd.data_source");
		
		List<DataSource> columns = jdbcTemplate.query(sb.toString(), 
				new RowMapper<DataSource>() {
			@Override
			public DataSource mapRow(ResultSet rs, int rowNum) throws SQLException {
				return new DataSource(null,  rs.getString("name"),  rs.getString("source_type"), 
						rs.getDate("version_date"), rs.getString("version_number"),
						rs.getString("organization_name"), rs.getString("data_source_category"), 
						rs.getString("geographic_coverage"), rs.getString("license"), rs.getString("full_name"),
						rs.getString("source"), rs.getString("source_id_field"));
			}});
		
		return columns;
	}
	
}
