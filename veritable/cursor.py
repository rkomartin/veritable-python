"""Cursor for paginated resource collections in the Veritable API.

See also: https://dev.priorknowledge.com/docs/client/python

"""

class Cursor:

    """Cursor for paginated resource collections in the Veritable API.

    See also: https://dev.priorknowledge.com/docs/client/python

    """
    def __init__(self, connection, collection, start=None, per_page=35,
        limit=None):
        self.__data = []
        self.__limit = limit
        self.__next = None
        self.__last = False
        self.__start = start
        self.__per_page = per_page
        self.__connection = connection
        self.__collection = collection

    @property 
    def collection(self):
        return self.__collection

    def _refresh(self):
        if len(self.__data):
            return len(self.__data)
        if self.__next:
            res = self.__connection.get(self.__next)
        elif self.__last:
            return 0
        else:
            params = {'count': self.__per_page}
            if self.__start:
                params.update({'start': self.__start})
            res = self.__connection.get(self.__collection, params=params)
        try:
            self.__next = res['links']['next']
        except KeyError:
            self.__next = None
            self.__last = True
        self.__data = res[self.__collection.split("/")[-1]]
        return len(self.__data)

    def __iter__(self):
        return self

    def next(self):
        if len(self.__data) or self._refresh():
            if self.__limit is not None:
                if self.__limit == 0:
                    raise StopIteration
                self.__limit -= 1
            return self.__data.pop(0)
        else:
            raise StopIteration
