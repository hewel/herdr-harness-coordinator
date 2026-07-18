ALTER TABLE tasks ADD COLUMN approved_result_revision INTEGER;
ALTER TABLE tasks ADD COLUMN approval_observation_id TEXT REFERENCES repository_observations(id);
