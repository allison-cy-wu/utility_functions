class DataValidityError(RuntimeError):
    pass


class EndDateTooLateError(RuntimeError):
    pass


class StartDateTooEarlyError(RuntimeError):
    pass


class OutputOutOfBoundError(RuntimeError):
    pass


class InputNotValidError(RuntimeError):
    pass

