-- Crear las tablas si no existen
CREATE TABLE IF NOT EXISTS AutoBackupJobs (
    id SERIAL PRIMARY KEY,
    job_name VARCHAR(255),
    source_path VARCHAR(4096) NOT NULL,
    destination_structure VARCHAR(4096) NOT NULL,
    frequency_hours INT NOT NULL CHECK (frequency_hours > 0)
);

CREATE TABLE IF NOT EXISTS BackupInstances (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    total_size BIGINT NOT NULL CHECK (total_size >= 0),
    user_defined_structure VARCHAR(4096) NOT NULL,
    auto_job_id INT,
    FOREIGN KEY (auto_job_id) REFERENCES AutoBackupJobs(id)
);

CREATE TABLE IF NOT EXISTS BackedUpFiles (
    id SERIAL PRIMARY KEY,
    backup_instance_id INT NOT NULL,
    path_within_source VARCHAR(4096) NOT NULL,
    size BIGINT NOT NULL CHECK (size >= 0),
    file_hash VARCHAR(64) NOT NULL,
    FOREIGN KEY (backup_instance_id) REFERENCES BackupInstances(id) ON DELETE CASCADE
);
