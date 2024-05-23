SQL_CREATE_ACCOUNTS_TABLE = """CREATE TABLE accounts (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    age TINYINT UNSIGNED,
                    birth_date DATE,
                    account_balance DECIMAL(10, 2),
                    is_active BOOLEAN,
                    signup_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_login DATETIME,
                    bio TEXT,
                    profile_picture BLOB,
                    favorite_color ENUM('red', 'green', 'blue'),
                    height FLOAT,
                    weight DOUBLE
                );"""
SQL_INSERT_ACCOUNTS_SAMPLE_DATA = """INSERT INTO accounts
(name, age, birth_date, account_balance, is_active, signup_time, last_login, bio, profile_picture, favorite_color, height, weight)
VALUES
('Alice', 30, '1991-05-21', 1500.00, TRUE, '2021-01-08 09:00:00', '2021-03-10 08:00:00', 'Bio of Alice', NULL, 'red', 1.70, 60.5);"""

SQL_CREATE_AUTHORS_TABLE = """CREATE TABLE authors (
    author_id INT AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    date_of_birth DATE,
    nationality VARCHAR(50),
    biography TEXT,
    email VARCHAR(255),
    phone_number VARCHAR(20),
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);"""

SQL_INSERT_AUTHORS_SAMPLE_DATA = """INSERT INTO authors (first_name, last_name, date_of_birth, nationality, biography, email, phone_number)
VALUES
('John', 'Doe', '1980-01-01', 'American', 'Biography of John Doe.', 'john.doe@example.com', '123-456-7890');"""

SQL_CREATE_NOVELS_TABLE = """CREATE TABLE novels (
    novel_id INT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    author_id INT,
    publish_date DATE,
    isbn VARCHAR(20),
    genre VARCHAR(100),
    page_count INT,
    publisher VARCHAR(100),
    language VARCHAR(50),
    available_copies INT,
    total_copies INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (author_id) REFERENCES authors(author_id)
);
"""
SQL_INSERT_NOVELS_SAMPLE_DATA = """INSERT INTO novels (title, author_id, publish_date, isbn, genre, page_count, publisher, language, available_copies, total_copies)
VALUES
('The Great Adventure', 1, '2020-06-01', '978-3-16-148410-0', 'Adventure', 300, 'Adventure Press', 'English', 10, 20),
('Journey to the Stars', 1, '2021-04-10', '978-0-11-322456-7', 'Science Fiction', 350, 'SciFi Universe', 'English', 12, 25);"""

ALTER_TABLES = [
    # control: column-type-change -> authors
    "ALTER TABLE authors MODIFY COLUMN email VARCHAR(100)",
    # control: drop-column -> accounts
    "ALTER TABLE accounts DROP COLUMN profile_picture;",
    # control: add-column with default value -> novels
    "ALTER TABLE novels ADD COLUMN is_stock BOOLEAN DEFAULT TRUE;",
]

CREATE_TABLES = [
    SQL_CREATE_AUTHORS_TABLE,
    SQL_CREATE_ACCOUNTS_TABLE,
    SQL_CREATE_NOVELS_TABLE,
]

DROP_TABLES = [
    "DROP TABLE IF EXISTS novels;",
    "DROP TABLE IF EXISTS accounts;",
    "DROP TABLE IF EXISTS authors;",
]

PRESEED_DATA = [
    SQL_INSERT_AUTHORS_SAMPLE_DATA,
    SQL_INSERT_ACCOUNTS_SAMPLE_DATA,
    SQL_INSERT_NOVELS_SAMPLE_DATA,
]
