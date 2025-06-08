-- Crear las tablas si no existen
CREATE TABLE IF NOT EXISTS AutoBackupJobs (
    id SERIAL PRIMARY KEY,
    job_name VARCHAR(255) NOT NULL,
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

-- Insertar datos de prueba
INSERT INTO BackupInstances (timestamp, total_size, user_defined_structure) VALUES
(NOW() - INTERVAL '2 day', 102400, 'documentos/importantes'),
(NOW() - INTERVAL '1 day', 512000, 'fotos/vacaciones'),
(NOW(), 20480, 'trabajo/urgente');