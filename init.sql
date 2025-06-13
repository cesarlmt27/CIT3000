-- Crear las tablas si no existen
CREATE TABLE IF NOT EXISTS AutoBackupJobs (
    id SERIAL PRIMARY KEY,
    job_name VARCHAR(255),
    source_path VARCHAR(4096) NOT NULL,
    destination_structure VARCHAR(4096) NOT NULL,
    frequency_hours INT NOT NULL CHECK (frequency_hours > 0),
    last_run_timestamp TIMESTAMP NULL
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

-- Datos de prueba para AutoBackupJobs
INSERT INTO AutoBackupJobs (job_name, source_path, destination_structure, frequency_hours, last_run_timestamp) VALUES
('Documentos Diarios', 'my_docs/files/universidad/tesis', 'backups/documentos_diarios', 24, NULL), -- Debería ejecutarse al iniciar
('Fotos Semanales', 'my_docs/files/universidad/mh.md', 'backups/fotos_semanales', 168, '2023-01-01 10:00:00'), -- Debería ejecutarse (fecha muy antigua)
('Codigo Importante Cada Hora', 'my_docs/files/universidad/pdi.md', 'backups/codigo_fuente', 1, NOW() - INTERVAL '2 hours'), -- Debería ejecutarse (hace 2 horas)
('Reportes Mensuales', 'my_docs/files/mercado.txt', 'backups/reportes_mensuales', 720, NOW() - INTERVAL '1 day'); -- No debería ejecutarse si frequency_hours es mayor a 24 y se corrió ayer. Ajustar según necesidad.

-- Ejemplo de un trabajo que NO debería ejecutarse inmediatamente si la frecuencia es, por ejemplo, de 24 horas.
INSERT INTO AutoBackupJobs (job_name, source_path, destination_structure, frequency_hours, last_run_timestamp) VALUES
('Backup Reciente', 'my_docs/files/password_manager.md', 'backups/recientes', 24, NOW() - INTERVAL '5 hours'); -- No se ejecutará si la frecuencia es 24h y solo han pasado 5h
