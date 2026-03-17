from flask import Flask, request
from flask_restx import Api, Resource, fields
from marshmallow import ValidationError
from werkzeug.serving import WSGIRequestHandler

from models import (
    BOOK_DATA,
    AUTHORS_DATA,
    get_all_books,
    get_all_authors,
    get_book_by_id,
    update_book_by_id,
    delete_book_by_id,
    get_all_author_books,
    get_author_by_id,
    get_author_by_name,
    delete_author_by_id,
    init_db,
    add_book,
    add_author
)
from schemas import BookSchema, AuthorSchema

USE_HTTP = True

app = Flask(__name__)
api = Api(
    app,
    title="BooksList",
    version="1.0.0",
    description="API for managing books",
    doc="/docs",
)

books_ns = api.namespace("books", description="Books operations", path="/api/books")
authors_ns = api.namespace("authors", description="Authors operations", path="/api/authors")


book_model = api.model(
    "Book",
    {
        "id": fields.Integer(),
        "title": fields.String(required=True, example="Harry Potter"),
        "author_id": fields.Integer(required=True, 
                                   description="Author ID, foreign key to Author.ID")
    }
)
# создание и обновление книги
book_create_model = api.model(
    "BookCreate",
    {
        "title": fields.String(required=True, example="Harry Potter"),
        "author_id": fields.Integer(required=True, example=1),
    }
)

author_model = api.model(
    "Author",
    {
        "id": fields.Integer(),
        "first_name": fields.String(required=True, example="Joanne"),
        "last_name": fields.String(required=True, example="Rowling"),
        "middle_name": fields.String(description="Optional")

    }
)

error_message = api.model(
    "Error",
    {
        "message": fields.String(example=["Book not found"])
    }
)

validation_error_model = api.model(
    "ValidationError",
    {
        "title": fields.String(example=["Not a valid string."])
    },
)

@books_ns.route('/')
class BookList(Resource):
    @books_ns.response(200, "All books")
    def get(self) -> tuple[list[dict], int]:
        schema = BookSchema()
        return schema.dump(get_all_books(), many=True)

    @books_ns.expect(book_create_model)
    @books_ns.response(201, "Book created")
    @books_ns.response(400, "Validation  error", validation_error_model)
    @books_ns.response(404, "Author not found", error_message)
    def post(self) -> tuple[dict, int]:
        "Добавляем книгу в ДБ"
        data = request.json
        schema = BookSchema()
        try:
            book = schema.load(data)
        except ValidationError as exc:
            return exc.messages, 400

        # проверка при создании книги, есть ли в ДБ такой автор
        if not get_author_by_id(book["author_id"]):
            return {"message": f"There is not author in the DB with id {book["author_id"]}"}, 404
        book = add_book(book)
        return schema.dump(book), 201


@authors_ns.route('/')
class AuthorsList(Resource):

    @authors_ns.response(200, "All authors")
    def get(self) -> tuple[list[dict], int]:
        """Вывести всех авторов из authors"""
        schema = AuthorSchema()
        return schema.dump(get_all_authors(), many=True), 200

    @authors_ns.expect(author_model)
    @authors_ns.response(201, "Author created")
    @authors_ns.response(400, "Validation  error", validation_error_model)
    @authors_ns.response(409, "There is already this author", error_message)
    def post(self) -> tuple[dict, int]:
        """Создаем нового автора"""
        data = request.json
        schema = AuthorSchema()
        try:
            author = schema.load(data)
        except ValidationError as exc:
            return exc.messages, 400
        
        # проверка, что автора нет в ДБ
        result = get_author_by_name(author["first_name"], author["last_name"])
        if result:
            return {"message": f"{author["first_name"], author["last_name"]} already exists in the DB"}, 409

        author = add_author(author)
        return schema.dump(author), 201
    
@books_ns.route('/<int:book_id>')
@books_ns.param("book_id")
class Books(Resource):

    schema = BookSchema()

    @books_ns.response(200, "Book by id")
    @books_ns.response(404, "Book is not found", error_message)
    def get(self, book_id) -> tuple[dict, int]:
        """Найти книгу по id в books"""
        result = get_book_by_id(book_id)
        if not result:
            return {'message': 'Book is not found in the DB'}, 404
        else:
            return self.schema.dump(result), 200
    
    @books_ns.expect(book_create_model)
    @books_ns.response(201, "Book is changed")
    @books_ns.response(400, "Validation  error", validation_error_model)
    def put(self, book_id) -> tuple[dict, int]:
        """Изменить книгу по id в таблице books
        В отличие от patch тут надо указать все атрибуты у книги, чтобы ее поменять в таблице"""
        data = request.json
        try:
            validated_data = self.schema.load(data)
        except ValidationError as exc:
            return exc.messages, 400
        
        update_book = update_book_by_id(book_id, validated_data)
        return self.schema.dump(update_book), 201
    
    @books_ns.expect(book_create_model)
    @books_ns.response(201, "Book is changed")
    @books_ns.response(409, "Validation  error", validation_error_model)
    @books_ns.response(404, "Book is not found", error_message)
    def patch(self, book_id) -> tuple[dict, int]:
        """Частично меняет у книги в таблице books один из параметров по id"""
        data = request.get_json()

        # проеврка, что пользователь вводит поля, которые есть в ДБ
        allowed_fields = {"title", "author_id"}
        unknown = set(data.keys()) - allowed_fields

        if unknown:
            return {"message": f"Unnoknw fields {unknown}"}, 409

        # проверка, что книга существует
        book = get_book_by_id(book_id)
        if not book:
            return {'message': f'{book_id} is not found in the DB'}, 404

        # обновляем только полученные поля
        if "title" in data:
            book.title = data["title"]
        if "author_id" in data:
            book.author_id = data["author_id"]
        
        update_book = update_book_by_id(book_id, book)
        return self.schema.dump(update_book), 201

    @books_ns.response(404, "Book is not found", error_message)
    @books_ns.response(201, "Book deleted succesfully")
    def delete(self, book_id) -> tuple[dict, int]:
        """Удаляет книгу по id"""
        # проверка, что книга существует
        book = get_book_by_id(book_id)
        if not book:
            return {'message': f'Book with id {book_id} is not found in the DB'}, 404
        
        result = delete_book_by_id(book_id)
        return {'message': f'{book.title} was deleted from the DB'}, 201


@authors_ns.route('/<int:author_id>')
@authors_ns.param("author_id")
class Authors(Resource):

    author_schema = AuthorSchema()
    book_schema = BookSchema()

    @authors_ns.marshal_list_with(book_model)
    @authors_ns.response(200, "Books of author are found")
    @authors_ns.response(404, "Author is not found", error_message)
    def get(self, author_id) -> tuple[dict, int] | tuple[list[dict], int]:
        """Просмотр всех книг автора"""
        
        result = get_all_author_books(author_id)
        if result:
            return self.book_schema.dump(result, many=True), 200
        else:
            return {"message": f"Author with id {author_id} has no books in the DB"}, 404

    @authors_ns.response(404, "Author is not found", error_message)
    @books_ns.response(201, "Author was deleted succesfully")
    def delete(self, author_id) -> tuple[dict, int]:
        """Удаляем автора и все его книги по его id"""
        # проверка, что автор есть в таблице authors
        author = get_author_by_id(author_id)
        if not author:
            return {"message": f"Author with id {author_id} is not found in the DB"}, 404

        result = delete_author_by_id(author_id)
        return {"message": f"Author with id {author_id} was deleted from the table as well as their books"}, 201


if __name__ == '__main__':
    init_db(initial_book_records=BOOK_DATA, initial_author_records=AUTHORS_DATA)
    
    if USE_HTTP:
        WSGIRequestHandler.protocol_version = "HTTP/1.1"
    # app.run(debug=True)
    # ТОЛЬКО для теста
    app.run(
        debug=False,
        threaded=True
    )

#    http://127.0.0.1:5000/docs

