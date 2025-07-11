# Amazon Books ETL DAG with Apache Airflow 📘

This project defines an **Apache Airflow DAG** to extract book information from Amazon search results and load it into a MySQL database.

---

## 🧠 What This DAG Does

The DAG performs the following steps:

1. **Extract**: Scrapes book data from Amazon's search results for "data engineering books".
2. **Transform**: Cleans and deduplicates the data using `pandas`.
3. **Load**: Inserts the cleaned data into a MySQL database table called `books`.

---

## 📦 Technologies Used

- **Airflow** for orchestrating the workflow
- **Python** for scraping and transformation
- **BeautifulSoup** for HTML parsing
- **MySQL** (via Airflow's `MySqlHook`) for data storage

---

## ⚙️ DAG Structure

| Task ID             | Description                              |
|---------------------|------------------------------------------|
| `fetch_book_data`   | Scrapes Amazon for book info             |
| `create_table`      | Creates the `books` table if not exists  |
| `insert_book_data`  | Inserts scraped data into MySQL          |

---

## 📋 MySQL Table Schema

The data is stored in a MySQL table with the following schema:

```sql
CREATE TABLE IF NOT EXISTS books (
    id INT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(255),
    author VARCHAR(255),
    price VARCHAR(50),
    rating VARCHAR(50)
);
```

---

## 🏁 How to Run

1. Add the MySQL connection in Airflow UI (Admin > Connections) with:
   - **Conn Id**: `books_connetion`
   - **Conn Type**: `MySQL`
   - Host, Schema, Login, Password: (your database details)

2. Place the DAG script in your Airflow `dags/` folder.

3. Start Airflow services:

```bash
airflow db init
airflow scheduler
airflow webserver
```

4. Trigger the DAG `etl_dag` from the Airflow UI.

---

