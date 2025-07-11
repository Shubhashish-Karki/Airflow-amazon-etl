from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta
import requests
import pandas as pd
from bs4 import BeautifulSoup
import logging

headers = {
    "Referer": 'https://www.amazon.com/',
    "Sec-Ch-Ua": "Not_A Brand",
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": "macOS",
    'User-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'
}

def get_amazon_data_books(num_books, ti):
    base_url = f"https://www.amazon.com/s?k=data+engineering+books"

    books = []  # Initialize as list
    seen_titles = set()
    page = 1

    while len(books) < num_books:
        url = f"{base_url}&page={page}"
        response = requests.get(url, headers=headers)

        # Check if the request was successful
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')

            book_items = soup.find_all("div", {"class": "a-section a-spacing-base"})
            logging.info(f"Found {len(book_items)} books on page {page}")

            for book in book_items:
                title = book.find("h2", {"class": "a-size-base-plus a-spacing-none a-color-base a-text-normal"})
                author = book.find("a", {"class": "a-size-base a-link-normal s-underline-text s-underline-link-text s-link-style"})
                price = book.find("span", {"class": "a-price-whole"})
                rating = book.find("span", {"class": "a-icon-alt"})

                logging.info("done with the books")

                if title and author and price and rating:
                    book_title = title.text.strip()

                    if book_title not in seen_titles:
                        seen_titles.add(book_title)
                        books.append({
                            "title": book_title,
                            "author": author.text.strip(),
                            "price": price.text.strip(),
                            "rating": rating.text.strip(),
                        })

            page += 1  # Increment page number

        else:
            print("Failed to fetch data from Amazon")
            break

    books = books[:num_books]
    df = pd.DataFrame(books)
    df.drop_duplicates(subset=['title'], inplace=True)

    ti.xcom_push(key='books_data', value=df.to_dict(orient='records'))



def insert_book_data_to_db(ti):

    book_data = ti.xcom_pull(key = 'books_data', task_ids='fetch_book_data')
    if not book_data:
        raise ValueError("No book data found in XCom")
    
    # Assuming you have a SQLAlchemy connection string
    logging.info("Inserting book data into the database...")
    db_hook = MySqlHook(mysql_conn_id='books_connetion')
    logging.info("Connected to the database successfully.")
    
    insert_query = """
    INSERT INTO books(title, author, price, rating)
    VALUES (%s, %s, %s, %s)
    """
    conn = db_hook.get_conn()
    cursor = conn.cursor()

    for book in book_data:
        cursor.execute(insert_query, (book['title'], book['author'], book['price'], book['rating']))

    conn.commit()
    cursor.close()
    conn.close()
    logging.info("Book data inserted into the database successfully.")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_dag',
    default_args=default_args,
    description='A simple ETL DAG to fetch data from amazon',
    schedule=None,
    catchup=False,
)


fetch_book_data = PythonOperator(
    task_id='fetch_book_data',
    python_callable=get_amazon_data_books,
    op_args=[10],
    dag=dag,
)

create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        sql="""
        CREATE TABLE IF NOT EXISTS books (
            id INT AUTO_INCREMENT PRIMARY KEY,
            title VARCHAR(255),
            author VARCHAR(255),
            price VARCHAR(50),
            rating VARCHAR(50)
        )
        """,
        conn_id='books_connetion',  # ✅ use conn_id, not mysql_conn_id here
    )

insert_book_data_task = PythonOperator(
    task_id='insert_book_data',
    python_callable=insert_book_data_to_db,
    dag=dag,
)

fetch_book_data >> create_table >> insert_book_data_task