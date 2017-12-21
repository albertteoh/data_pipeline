import gzip
import pytest
import data_pipeline.constants.const as const

from data_pipeline.db.connection_details import ConnectionDetails
from data_pipeline.db.filedb import FileDb
from data_pipeline.stream.file_writer import FileWriter

CSV = "CSV"
PSV = "PSV"
CSV_GZ = "GZ"
CSV_BZ2 = "BZ2"
CSV_NOEXT = "NOEXT"
CSV_LOWERCASE = "lowercase"


@pytest.fixture
def setup(tmpdir):
    data_dir = tmpdir.mkdir("d")
    data_file_csv = str(data_dir.join("{}.csv".format(CSV)))
    data_file_gz = str(data_dir.join("{}.csv.gz".format(CSV_GZ)))
    data_file_bz2 = str(data_dir.join("{}.csv.bz2".format(CSV_BZ2)))
    data_file_noext  = str(data_dir.join("{}".format(CSV_NOEXT)))
    data_file_lowercase = str(data_dir.join("{}.csv".format(CSV_LOWERCASE.lower())))
    data_file_psv = str(data_dir.join("{}.psv".format(PSV)))

    write_csv_records_to_file(data_file_csv)
    write_csv_records_to_file(data_file_gz)
    write_csv_records_to_file(data_file_bz2)
    write_csv_records_to_file(data_file_noext)
    write_csv_records_to_file(data_file_lowercase)
    write_psv_records_to_file(data_file_psv)

    filedb = FileDb()
    connection_details = ConnectionDetails(data_dir=str(data_dir))
    filedb.connect(connection_details)

    yield(filedb)


def write_csv_records_to_file(filename):
    file_writer = FileWriter(filename)
    file_writer.writeln("a0,b0,c0")
    file_writer.writeln('"a1",b1,c1')
    file_writer.writeln('a2,"b2,withdelim",c2')
    file_writer.flush()
    file_writer.close()


def write_psv_records_to_file(filename):
    file_writer = FileWriter(filename)
    file_writer.writeln("a0|b0|c0")
    file_writer.writeln('"a1"|b1|c1')
    file_writer.writeln('a2|"b2|withdelim"|c2')
    file_writer.flush()
    file_writer.close()


def replace_as(record):
    return [v.replace('a', 'foo') for v in record]


@pytest.mark.parametrize("delim, quotechar, samplerows, arraysize, post_process_func, expected_result", [

    (const.COMMA, None, None, 2, None, [
        ["a0", "b0", "c0"],
        ['"a1"', "b1", "c1"],
        ["a2", '"b2', 'withdelim"', "c2"]]),
    (const.COMMA, None, None, 2, replace_as, [
        ["foo0", "b0", "c0"],
        ['"foo1"', "b1", "c1"],
        ["foo2", '"b2', 'withdelim"', "c2"]]),
    (const.COMMA, '"', None, 2, None, [
        ["a0", "b0", "c0"],
        ["a1", "b1", "c1"],
        ["a2", "b2,withdelim", "c2"]]),
    (const.COMMA, '"', None, 2, replace_as, [
        ["foo0", "b0", "c0"],
        ["foo1", "b1", "c1"],
        ["foo2", "b2,withdelim", "c2"]]),
    (const.COMMA, '"', 2, 2, None, [
        ["a0", "b0", "c0"],
        ["a1", "b1", "c1"]]),
    (const.COMMA, '"', 100, 2, None, [
        ["a0", "b0", "c0"],
        ["a1", "b1", "c1"],
        ["a2", "b2,withdelim", "c2"]]),
    (const.COMMA, '"', 100, 2, replace_as, [
        ["foo0", "b0", "c0"],
        ["foo1", "b1", "c1"],
        ["foo2", "b2,withdelim", "c2"]]),

    (const.PIPE, None, None, 2, None, [
        ["a0", "b0", "c0"],
        ['"a1"', "b1", "c1"],
        ["a2", '"b2', 'withdelim"', "c2"]]),
    (const.PIPE, None, None, 2, replace_as, [
        ["foo0", "b0", "c0"],
        ['"foo1"', "b1", "c1"],
        ["foo2", '"b2','withdelim"', "c2"]]),
    (const.PIPE, '"', None, 2, None, [
        ["a0", "b0", "c0"],
        ["a1", "b1", "c1"],
        ["a2", "b2|withdelim", "c2"]]),
    (const.PIPE, '"', None, 2, replace_as, [
        ["foo0", "b0", "c0"],
        ["foo1", "b1", "c1"],
        ["foo2", "b2|withdelim", "c2"]]),
    (const.PIPE, '"', 2, 2, None, [
        ["a0", "b0", "c0"],
        ["a1", "b1", "c1"]]),
    (const.PIPE, '"', 100, 2, None, [
        ["a0", "b0", "c0"],
        ["a1", "b1", "c1"],
        ["a2", "b2|withdelim", "c2"]]),
    (const.PIPE, '"', 100, 2, replace_as, [
        ["foo0", "b0", "c0"],
        ["foo1", "b1", "c1"],
        ["foo2", "b2|withdelim", "c2"]]),
])
def test_execute_query(delim, quotechar, samplerows, arraysize,
                       post_process_func, expected_result, mocker, setup):
    (filedb) = setup
    filedb.delimiter = delim
    filedb.quotechar = quotechar
    filedb.samplerows = samplerows
    tables = None

    if delim == const.COMMA:
        tables = [
            CSV,
            CSV_GZ,
            CSV_BZ2,
            CSV_NOEXT,
            CSV_LOWERCASE.upper(), # test case insensitivity
        ]

    elif delim == const.PIPE:
        tables = [
            PSV,
        ]

    map(lambda t: execute_query_on_table(filedb, t, arraysize,
                    post_process_func, expected_result), tables)


def execute_query_on_table(filedb, tablename, arraysize,
                           post_process_func, expected_result):
    results = filedb.execute_query(tablename,
                                   arraysize,
                                   post_process_func=post_process_func)
    assert results.fetchall() == expected_result
