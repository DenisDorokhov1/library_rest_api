import sqlite3
from dataclasses import dataclass
from typing import Optional, Union, List, Dict

BOOK_DATA = [
    {'title': 'A Byte of Python', 'author_id': 1},
    {'title': 'Moby-Dick; or, The Whale', 'author_id': 2},
    {'title': 'War and Peace', 'author_id': 3},
]

AUTHORS_DATA = [
    {'first_name': 'Chitlur', 'last_name': 'Swaroop', 'middle_name': None},
    {'first_name': 'Herman', 'last_name': 'Melville', 'middle_name': None},
    {'first_name': 'Leo', 'last_name': 'Tolstoy', 'middle_name': 'Nikolaevich'}
]

DATABASE_NAME = 'table_books.db'
BOOKS_TABLE_NAME = 'books'
AUTHORS_TABLE_NAME = 'authors'

@dataclass
class Book:
    title: str
    author_id: int
    id: Optional[int] = None

    def __getitem__(self, item: str) -> Union[int, str]:
        return getattr(self, item)

@dataclass
class Author:
    first_name: str
    last_name: str
    middle_name: Optional[str] = None
    id: Optional[int] = None

    def __getitem__(self, item: str) -> Union[int, str]:
        return getattr(self, item)
    

def init_db(initial_book_records: List[Dict], initial_author_records: List[Dict]) -> None:
    with _get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='{BOOKS_TABLE_NAME}';
            """
        )
        exists = cursor.fetchone()
        if not exists:
            cursor.executescript(
                f"""
                CREATE TABLE `{BOOKS_TABLE_NAME}`(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, 
                    title TEXT,
                    author_id INTEGER NOT NULL REFERENCES {AUTHORS_TABLE_NAME} (id) ON DELETE CASCADE
                );
                """
            )
            cursor.executemany(
                f"""
                INSERT INTO `{BOOKS_TABLE_NAME}`
                (title, author_id) VALUES (?, ?)
                """,
                [
                    (item['title'], item['author_id'])
                    for item in initial_book_records
                ]
            )
        
        cursor.execute(
            f"""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='{AUTHORS_TABLE_NAME}';
            """
        )
        exists = cursor.fetchone()
        if not exists:
            cursor.executescript(
                f"""
                CREATE TABLE `{AUTHORS_TABLE_NAME}`(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, 
                    first_name VARCHAR(255) NOT NULL,
                    last_name VARCHAR(255) NOT NULL,
                    middle_name VARCHAR(255)
                );
                """
            )
            cursor.executemany(
                f"""
                INSERT INTO `{AUTHORS_TABLE_NAME}`
                (first_name, last_name, middle_name) VALUES (?, ?, ?)
                """,
                [
                    (item['first_name'], item['last_name'], item['middle_name'])
                    for item in initial_author_records
                ]
            )


def _get_book_obj_from_row(row: tuple) -> Book:
    return Book(id=row[0], title=row[1], author_id=row[2])


def _get_author_obj_from_row(row: tuple) -> Author:
    return Author(id=row[0], first_name=row[1], last_name=row[2], middle_name=row[3])


def get_all_books() -> list[Book]:
    with _get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(f'SELECT * FROM `{BOOKS_TABLE_NAME}`')
        all_books = cursor.fetchall()
        return [_get_book_obj_from_row(row) for row in all_books]


def get_all_authors() -> list[Author]:
    with _get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(f'SELECT * FROM `{AUTHORS_TABLE_NAME}`')
        all_authors = cursor.fetchall()
        return [_get_author_obj_from_row(row) for row in all_authors]

def add_book(book: Book) -> Book:
    with _get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            INSERT INTO `{BOOKS_TABLE_NAME}` 
            (title, author_id) VALUES (?, ?)
            """,
            (book.title, book.author_id)
        )
        book.id = cursor.lastrowid
        # получаем id последние строки, которую только что вставили
        return book
    

def add_author(author: Author) -> Author:
    with _get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            INSERT INTO `{AUTHORS_TABLE_NAME}` 
            (first_name, last_name, middle_name) VALUES (?, ?, ?)
            """,
            (author.first_name, author.last_name, author.middle_name)
        )
        author.id = cursor.lastrowid
        return author


def get_book_by_id(book_id: int) -> Optional[Book]:
    with _get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT * FROM `{BOOKS_TABLE_NAME}` WHERE id = ?
            """,
            (book_id,)
        )
        book = cursor.fetchone()
        if book:
            return _get_book_obj_from_row(book)


def update_book_by_id(book_id: int, book: Book) -> None:
    with _get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            UPDATE {BOOKS_TABLE_NAME}
            SET title = ?, author_id = ?
            WHERE id = ?
            """,
            (book.title, book.author_id, book_id)
        )
        
        cursor.execute(
            f"""
            SELECT * FROM {BOOKS_TABLE_NAME}
            WHERE id = ?
            """,
            (book_id,)
        )
        updated_row = cursor.fetchone()

        if updated_row:
            return _get_book_obj_from_row(updated_row)



def delete_book_by_id(book_id: int) -> None:
    with _get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            DELETE FROM {BOOKS_TABLE_NAME}
            WHERE id = ?
            """,
            (book_id,)
        )


def get_book_by_title(book_title: str) -> Optional[Book]:
    with _get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT * FROM `{BOOKS_TABLE_NAME}` WHERE title = ?
            """,
            (book_title,)
        )
        book = cursor.fetchone()
        if book:
            return _get_book_obj_from_row(book)


def get_all_author_books(author_id: int) -> Optional[Book]:
    with _get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT * FROM `{BOOKS_TABLE_NAME}` WHERE author_id = ?
            """,
            (author_id,)
        )
        books = cursor.fetchall()
        if books:
            return [_get_book_obj_from_row(row) for row in books]


def get_author_by_id(author_id: int) -> Optional[Author]:
    with _get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT * FROM `{AUTHORS_TABLE_NAME}` WHERE id = ?
            """,
            (author_id,)
        )
        author = cursor.fetchone()
        if author:
            return _get_author_obj_from_row(author)


def get_author_by_name(author_name: str, author_last_name: str) -> Optional[Author]:
    with _get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT * FROM `{AUTHORS_TABLE_NAME}` 
            WHERE first_name = ? and last_name = ?
            """,
            (author_name, author_last_name)
        )
        author = cursor.fetchone()
        if author:
            return _get_author_obj_from_row(author)


def delete_author_by_id(author_id: int) -> None:
    with _get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            DELETE FROM {AUTHORS_TABLE_NAME}
            WHERE id = ?
            """,
            (author_id,)
        )

def _get_connection():
    """Позволяет установить связь с таблицами"""
    conn = sqlite3.connect(DATABASE_NAME)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn