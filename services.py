from psycopg2 import connect, extras
from uuid import uuid4


class Postgres:
    def __init__(self, user, password, dbname, host, port, stream=False):
        self.user = user
        self.password = password
        self.dbname = dbname
        self.host = host
        self.port = port
        self.stream = stream
        self.name = None
        if stream:
            self.name = uuid4().hex
        pass

    def __enter__(self):
        self.connection = connect(
            f'dbname={self.dbname} user={self.user} password={self.password} host={self.host} port={self.port}')
        self.connection.autocommit = not self.stream
        self.cursor = self.connection.cursor(cursor_factory=extras.DictCursor, name=self.name)
        return self.cursor

    def __exit__(self, exception_type, exception_value, traceback):
        self.cursor.close()
        if self.stream:
            if exception_value:
                self.connection.rollback()
            else:
                self.connection.commit()
        self.connection.close()
