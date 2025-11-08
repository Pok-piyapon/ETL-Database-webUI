-- Sample schema and data for source DB
CREATE TABLE IF NOT EXISTS customers (
  id INT PRIMARY KEY AUTO_INCREMENT,
  name VARCHAR(200) NOT NULL,
  email VARCHAR(200) UNIQUE,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

INSERT INTO customers (name, email) VALUES
 ('Alice','alice@example.com'),
 ('Bob','bob@example.com')
ON DUPLICATE KEY UPDATE name=VALUES(name);
