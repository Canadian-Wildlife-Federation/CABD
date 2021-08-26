UPDATE waterfalls.waterfalls SET complete_level_code = 
    CASE
    WHEN ((fall_name_en IS NOT NULL OR fall_name_fr IS NOT NULL) AND fall_height_m IS NOT NULL) THEN 3
    WHEN ((fall_name_en IS NOT NULL OR fall_name_fr IS NOT NULL) OR fall_height_m IS NOT NULL) THEN 2
    ELSE 1 END;