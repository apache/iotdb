from sqlalchemy.sql.compiler import IdentifierPreparer


class IoTDBIdentifierPreparer(IdentifierPreparer):
    def __init__(self, dialect, **kw):
        quote = '`'
        super(IoTDBIdentifierPreparer, self).__init__(
            dialect, initial_quote=quote, escape_quote=quote, **kw
        )
