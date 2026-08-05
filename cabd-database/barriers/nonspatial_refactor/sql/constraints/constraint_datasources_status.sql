ALTER TABLE IF EXISTS cabd.data_source_updates
    ADD COLUMN update_status character varying COLLATE pg_catalog."default";

ALTER TABLE IF EXISTS cabd.data_source_updates
    ADD CONSTRAINT status_check CHECK (update_status::text = ANY (ARRAY['needs review'::character varying::text, 'ready'::character varying::text, 'wait'::character varying::text, 'blocked'::character varying::text, 'done'::character varying::text]));