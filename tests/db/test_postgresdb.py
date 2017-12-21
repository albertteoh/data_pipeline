import pytest

import data_pipeline.constants.const as const

from pytest_mock import mocker
from data_pipeline.db.postgresdb import PostgresDb
from data_pipeline.db.connection_details import ConnectionDetails


@pytest.fixture
def setup(mocker):
    db = PostgresDb()

    connection_details = ConnectionDetails(
        userid="myuserid", password="mypassword",
        host="myhost", port=1234, dbsid="mydbsid")

    mock_cursor_config = {
        "execute.return_value": None,
        "rowcount": 99,
        "description": ['a', 'b']
    }
    mock_cursor = mocker.Mock(**mock_cursor_config)

    def connect_se(connection_details):
        connected = True

    def close_se():
        connected = False

    mock_connection_config = {
        "connect.side_effect": connect_se,
        "close.side_effect": close_se,
        "cursor.return_value": mock_cursor,
        "closed": 0,
    }

    mock_connection = mocker.Mock(**mock_connection_config)

    def makedsn_se(host, port, dbsid):
        return cx_Oracle.makedsn(host, port, dbsid)

    mock_psycopg2 = mocker.patch("data_pipeline.db.postgresdb.psycopg2")
    mock_psycopg2.makedsn.side_effect = makedsn_se
    mock_psycopg2.connect.return_value = mock_connection

    yield(db, connection_details, mock_psycopg2, mock_connection, mock_cursor)


@pytest.mark.parametrize("nullstring, header, assumematchingcolumns", [
    (const.EMPTY_STRING, False, False),
    (const.NULL, False, False),
    (const.NULL, True, False),
    (const.NULL, True, True),
])
def test_copy_expert(nullstring, header, assumematchingcolumns,
                     mocker, setup, tmpdir):
    filepath = tmpdir.mkdir("test").join("input.txt")
    f = filepath.open("a")
    (db, connection_details, mock_psycopg2, mock_connection, mock_cursor) = setup
    db.connect(connection_details)
    size = 100
    if assumematchingcolumns:
        column_list = None
        expected_column_list = const.EMPTY_STRING
    else:
        column_list = ["col0", "col1", "col2"]
        expected_column_list = "( col0,col1,col2 )"

    db.copy_expert(
        input_file=f,
        table_name="tableA",
        sep=const.COMMA,
        null_string=nullstring,
        column_list=column_list, 
        quote_char='"',
        escape_char='\\',
        size=size,
        header=header,
    )

    mock_cursor.copy_expert.assert_called_once_with(
        """
            COPY tableA {expected_column_list}
            FROM STDIN
            DELIMITER \',\' CSV
            NULL '{nullstring}'
            QUOTE \'"\'
            ESCAPE \'\\\'
            {header}
        """.format(
                nullstring=nullstring,
                expected_column_list=expected_column_list,
                header="HEADER" if header else const.EMPTY_STRING,
            ),
        mocker.ANY,
        size=size,
                   
    )
    f.close()
