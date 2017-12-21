###############################################################################
# Module:    file_query_results
# Purpose:   Represents the query result object returned from a file query
#
# Notes:
###############################################################################

import itertools
import csv

from .query_results import QueryResults
from data_pipeline.stream.file_reader import FileReader


def default_post_process_func(record):
    return record


class FileQueryResults(QueryResults):

    def __init__(self, filename, delimiter, quotechar,
                 samplerows, post_process_func):
        super(FileQueryResults, self).__init__()
        
        self._handle = FileReader(filename)
        self._csvreader = csv.reader(self._handle,
                                     delimiter=delimiter,
                                     quotechar=quotechar,)
        self._samplerows = samplerows
        self._rowcount = 0

        if post_process_func is None:
            self._post_process_func = default_post_process_func
        else:
            self._post_process_func = post_process_func

    def __iter__(self):
        return self

    def next(self):
        if self._samplerows is not None and self._rowcount >= self._samplerows:
            self._handle.close()
            raise StopIteration

        record = self._csvreader.next()
        if not record:
            self._handle.close()
            raise StopIteration

        self._rowcount += 1

        return self._post_process_func(record)

    def fetchone(self):
        record = None
        try:
            record = self.next()
        except StopIteration, e:
            pass
        return record

    def fetchall(self):
        if self._samplerows is not None:
            result = [self._post_process_func(l)
                      for l in itertools.islice(self._csvreader, self._samplerows)]
        else:
            result = [self._post_process_func(l) for l in self._csvreader]

        self._rowcount += len(result)
        return result

    def fetchmany(self, arraysize=None):
        if arraysize > 0:
            if (self._samplerows is not None and
                (self._rowcount + arraysize) > self._samplerows):
                remainder = self._samplerows - self._rowcount
                result = [self._post_process_func(l)
                          for l in itertools.islice(self._csvreader, remainder)]
            else:
                result = [self._post_process_func(l)
                          for l in itertools.islice(self._csvreader, arraysize)]
        else:
            result = self.fetchall()

        self._rowcount += len(result)
        return result

    def __del__(self):
        self._handle.close()
