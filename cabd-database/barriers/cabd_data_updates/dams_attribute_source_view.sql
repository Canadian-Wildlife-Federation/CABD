SELECT foo.cabd_id,
    dne.name AS dam_name_en_ds_text,
    dnf.name AS dam_name_fr_ds_text,
    wne.name AS waterbody_name_en_ds_text,
    wnf.name AS waterbody_name_fr_ds_text,
    rne.name AS reservoir_name_en_ds_text,
    rnf.name AS reservoir_name_fr_ds_text,
    o.name AS owner_ds_text,
    otc.name AS ownership_type_code_ds_text,
    pcs.name AS provincial_compliance_status_ds_text,
    fcs.name AS federal_compliance_status_ds_text,
    ont.name AS operating_notes_ds_text,
    osc.name AS operating_status_code_ds_text,
    uc.name AS use_code_ds_text,
    ui.name AS use_irrigation_code_ds_text,
    ue.name AS use_electricity_code_ds_text,
    us.name AS use_supply_code_ds_text,
    ufc.name AS use_floodcontrol_code_ds_text,
    ur.name AS use_recreation_code_ds_text,
    un.name AS use_navigation_code_ds_text,
    uf.name AS use_fish_code_ds_text,
    up.name AS use_pollution_code_ds_text,
    uin.name AS use_invasivespecies_code_ds_text,
    uo.name AS use_other_code_ds_text,
    lc.name AS lake_control_code_ds_text,
    cy.name AS construction_year_ds_text,
    asch.name AS assess_schedule_ds_text,
    el.name AS expected_life_ds_text,
    ml.name AS maintenance_last_ds_text,
    mn.name AS maintenance_next_ds_text,
    fcode.name AS function_code_ds_text,
    cc.name AS condition_code_ds_text,
    ctc.name AS construction_type_code_ds_text,
    hm.name AS height_m_ds_text,
    lm.name AS length_m_ds_text,
    scc.name AS size_class_code_ds_text,
    scap.name AS spillway_capacity_ds_text,
    stc.name AS spillway_type_code_ds_text,
    rp.name AS reservoir_present_ds_text,
    ra.name AS reservoir_area_skm_ds_text,
    rd.name AS reservoir_depth_m_ds_text,
    scm.name AS storage_capacity_mcm_ds_text,
    ard.name AS avg_rate_of_discharge_ls_ds_text,
    dor.name AS degree_of_regulation_pc_ds_text,
    pfr.name AS provincial_flow_req_ds_text,
    ffr.name AS federal_flow_req_ds_text,
    ca.name AS catchment_area_skm_ds_text,
    ul.name AS upstream_linear_km_ds_text,
    hps.name AS hydro_peaking_system_ds_text,
    gc.name AS generating_capacity_mwh_ds_text,
    tn.name AS turbine_number_ds_text,
    tt.name AS turbine_type_code_ds_text,
    uptc.name AS up_passage_type_code_ds_text,
    dprc.name AS down_passage_route_code_ds_text,
    psc.name AS passability_status_code_ds_text,
    psn.name AS passability_status_note_ds_text,
    c.name AS comments_ds_text,
    clc.name AS complete_level_code_ds_text,
    op.name AS original_point_ds_text
FROM featurecopy.dams_attribute_source foo
    LEFT JOIN cabd.data_source dne ON dne.id = foo.dam_name_en_ds
    LEFT JOIN cabd.data_source dnf ON dnf.id = foo.dam_name_fr_ds
    LEFT JOIN cabd.data_source wne ON wne.id = foo.waterbody_name_en_ds
    LEFT JOIN cabd.data_source wnf ON wnf.id = foo.waterbody_name_fr_ds
    LEFT JOIN cabd.data_source rne ON rne.id = foo.reservoir_name_en_ds
    LEFT JOIN cabd.data_source rnf ON rnf.id = foo.reservoir_name_fr_ds
    LEFT JOIN cabd.data_source o ON o.id = foo.owner_ds
    LEFT JOIN cabd.data_source otc ON otc.id = foo.ownership_type_code_ds
    LEFT JOIN cabd.data_source pcs ON pcs.id = foo.provincial_compliance_status_ds
    LEFT JOIN cabd.data_source fcs ON fcs.id = foo.federal_compliance_status_ds
    LEFT JOIN cabd.data_source ont ON ont.id = foo.operating_notes_ds
    LEFT JOIN cabd.data_source osc ON osc.id = foo.operating_status_code_ds
    LEFT JOIN cabd.data_source uc ON uc.id = foo.use_code_ds
    LEFT JOIN cabd.data_source ui ON ui.id = foo.use_irrigation_code_ds
    LEFT JOIN cabd.data_source ue ON ue.id = foo.use_electricity_code_ds
    LEFT JOIN cabd.data_source us ON us.id = foo.use_supply_code_ds
    LEFT JOIN cabd.data_source ufc ON ufc.id = foo.use_floodcontrol_code_ds
    LEFT JOIN cabd.data_source ur ON ur.id = foo.use_recreation_code_ds
    LEFT JOIN cabd.data_source un ON un.id = foo.use_navigation_code_ds
    LEFT JOIN cabd.data_source uf ON uf.id = foo.use_fish_code_ds
    LEFT JOIN cabd.data_source up ON up.id = foo.use_pollution_code_ds
    LEFT JOIN cabd.data_source uin ON uin.id = foo.use_invasivespecies_code_ds
    LEFT JOIN cabd.data_source uo ON uo.id = foo.use_other_code_ds
    LEFT JOIN cabd.data_source lc ON lc.id = foo.lake_control_code_ds
    LEFT JOIN cabd.data_source cy ON cy.id = foo.construction_year_ds
    LEFT JOIN cabd.data_source asch ON asch.id = foo.assess_schedule_ds
    LEFT JOIN cabd.data_source el ON el.id = foo.expected_life_ds
    LEFT JOIN cabd.data_source ml ON ml.id = foo.maintenance_last_ds
    LEFT JOIN cabd.data_source mn ON mn.id = foo.maintenance_next_ds
    LEFT JOIN cabd.data_source fcode ON fcode.id = foo.function_code_ds
    LEFT JOIN cabd.data_source cc ON cc.id = foo.condition_code_ds
    LEFT JOIN cabd.data_source ctc ON ctc.id = foo.construction_type_code_ds
    LEFT JOIN cabd.data_source hm ON hm.id = foo.height_m_ds
    LEFT JOIN cabd.data_source lm ON lm.id = foo.length_m_ds
    LEFT JOIN cabd.data_source scc ON scc.id = foo.size_class_code_ds
    LEFT JOIN cabd.data_source scap ON scap.id = foo.spillway_capacity_ds
    LEFT JOIN cabd.data_source stc ON stc.id = foo.spillway_type_code_ds
    LEFT JOIN cabd.data_source rp ON rp.id = foo.reservoir_present_ds
    LEFT JOIN cabd.data_source ra ON ra.id = foo.reservoir_area_skm_ds
    LEFT JOIN cabd.data_source rd ON rd.id = foo.reservoir_depth_m_ds
    LEFT JOIN cabd.data_source scm ON scm.id = foo.storage_capacity_mcm_ds
    LEFT JOIN cabd.data_source ard ON ard.id = foo.avg_rate_of_discharge_ls_ds
    LEFT JOIN cabd.data_source dor ON dor.id = foo.degree_of_regulation_pc_ds
    LEFT JOIN cabd.data_source pfr ON pfr.id = foo.provincial_flow_req_ds
    LEFT JOIN cabd.data_source ffr ON ffr.id = foo.federal_flow_req_ds
    LEFT JOIN cabd.data_source ca ON ca.id = foo.catchment_area_skm_ds
    LEFT JOIN cabd.data_source ul ON ul.id = foo.upstream_linear_km_ds
    LEFT JOIN cabd.data_source hps ON hps.id = foo.hydro_peaking_system_ds
    LEFT JOIN cabd.data_source gc ON gc.id = foo.generating_capacity_mwh_ds
    LEFT JOIN cabd.data_source tn ON tn.id = foo.turbine_number_ds
    LEFT JOIN cabd.data_source tt ON tt.id = foo.turbine_type_code_ds
    LEFT JOIN cabd.data_source uptc ON uptc.id = foo.up_passage_type_code_ds
    LEFT JOIN cabd.data_source dprc ON dprc.id = foo.down_passage_route_code_ds
    LEFT JOIN cabd.data_source psc ON psc.id = foo.passability_status_code_ds
    LEFT JOIN cabd.data_source psn ON psn.id = foo.passability_status_note_ds
    LEFT JOIN cabd.data_source c ON c.id = foo.comments_ds
    LEFT JOIN cabd.data_source clc ON clc.id = foo.complete_level_code_ds
    LEFT JOIN cabd.data_source op ON op.id = foo.original_point_ds;