import pytest
from data_pipeline.db.file_query_results import FileQueryResults
from data_pipeline.stream.file_writer import FileWriter


@pytest.fixture
def setup(tmpdir):
    data_file = tmpdir.mkdir("d").join("data")
    file_writer = FileWriter(str(data_file))
    file_writer.writeln("a0,b0,c0")
    file_writer.writeln("a1,b1,c1")
    file_writer.writeln("a2,b2,c2")
    file_writer.flush()
    file_writer.close()
    yield(str(data_file))


def test_fetchone(setup):
    (data_file) = setup
    file_query_results = FileQueryResults(data_file, ',', '"', None, None)
    assert file_query_results.fetchone() == ["a0", "b0", "c0"]


def test_fetchmany(setup):
    (data_file) = setup
    file_query_results = FileQueryResults(data_file, ',', '"', None, None)
    assert file_query_results.fetchmany(arraysize=2) == [["a0", "b0", "c0"],
                                                         ["a1", "b1", "c1"]]


@pytest.mark.parametrize("samplerows, expected_result", [
    (None, [
        ["a0", "b0", "c0"],
        ["a1", "b1", "c1"],
        ["a2", "b2", "c2"]]),
    (2, [
        ["a0", "b0", "c0"],
        ["a1", "b1", "c1"]]),
])
def test_fetchall(samplerows, expected_result, setup):
    (data_file) = setup
    file_query_results = FileQueryResults(data_file, ',', '"', samplerows, None)
    assert file_query_results.fetchall() == expected_result


def test_foreach(setup):
    (data_file) = setup
    file_query_results = FileQueryResults(data_file, ',', '"', None, None)
    lines = [l for l in file_query_results]
    assert lines == [["a0", "b0", "c0"],
                     ["a1", "b1", "c1"],
                     ["a2", "b2", "c2"]]
