

class AstronomerException(Exception):
    pass


class AstronomerConnectionException(AstronomerException):
    pass


class AstronomerFieldException(AstronomerException):
    pass


class AstronomerEntityException(AstronomerException):
    pass


class AstronomerMapperException(AstronomerException):
    pass


class AstronomerQueryException(AstronomerException):
    pass
