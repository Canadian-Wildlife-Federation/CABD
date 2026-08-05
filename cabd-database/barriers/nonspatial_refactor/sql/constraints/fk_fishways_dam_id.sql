ALTER TABLE fishways.fishways
  DROP CONSTRAINT IF EXISTS fishways_dam_id_fkey;

ALTER TABLE fishways.fishways
  ADD CONSTRAINT fishways_dam_id_fkey
  FOREIGN KEY (dam_id)
  REFERENCES dams.dams(cabd_id)
  ON DELETE SET NULL;
