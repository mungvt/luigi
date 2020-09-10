import json
import os

import pandas
from mock import patch
from nose2.tools import such

import logic

with such.A('logic') as it:
    with it.having("read json file"):
        @it.has_setup
        def setup():
            it.data_test = [
                {"apple": "red",
                 "banana": "yellow",
                 "cherry": "purple"
                 }
            ]
            with open('data_test.json', 'w') as outfile:
                json.dump(
                    it.data_test,
                    outfile
                )

        @it.has_teardown
        def teardown():
            os.remove('data_test.json')

        @it.should('Read successfully a json file')
        def test_read_json_file():
            result = logic.read_file_json('data_test.json')
            print(result)
            it.assertListEqual(
                it.data_test,
                result)

    with it.having('import data to mysql'):
        @it.should('Import successfully the database')
        @patch('logic.read_file_json')
        @patch('logic.session.commit')
        def test_import_data(mock_session_commit, mock_read_file_json):
            # Given
            mock_read_file_json.return_value = []
            # When
            logic.import_data_to_mysql()
            # Then
            mock_read_file_json.asserst_called_once()
            mock_session_commit.assert_called_once()

    with it.having("delayed flight airport"):
        @it.should('Top 10 delayed flight Airport!')
        @patch('logic.session.query')
        @patch('logic.session.commit')
        def test_delayed_flight_airport(mock_session_commit, mock_session_query):
            # When
            logic.create_top10_delayed_flight_airports()
            # Then
            mock_session_query.assert_called()
            mock_session_commit.assert_called_once()

    with it.having("delayed minute airport"):
        @it.should('Top 10 delayed minute Airport!')
        @patch('logic.session.query')
        @patch('logic.session.commit')
        def test_delayed_minute_airport(mock_session_commit, mock_session_query):
            # When
            logic.create_top10_delayed_minute_airports()
            # Then
            mock_session_query.assert_called()
            mock_session_commit.assert_called_once()

    with it.having("delayed flight cities"):
        @it.should('Top 10 delayed flight cities!')
        @patch('logic.session.query')
        @patch('logic.session.commit')
        def test_delayed_flight_cities(mock_session_commit, mock_session_query):
            # When
            logic.create_top10_delayed_flight_cities()
            # Then
            mock_session_query.assert_called()
            mock_session_commit.assert_called_once()

    with it.having("delayed minute airport cities"):
        @it.should('Top 10 delayed minute airport cities!')
        @patch('logic.session.query')
        @patch('logic.session.commit')
        def test_delayed_minute_airport_cities(mock_session_commit, mock_session_query):
            # When
            logic.create_top10_delayed_minute_airport_cities()
            # Then
            it.assertEqual(mock_session_query.call_count, 3)
            mock_session_commit.assert_called_once()

    with it.having("delayed flight carrier"):
        @it.should('Top 10 delayed flight carrier!')
        @patch('logic.session.query')
        @patch('logic.session.commit')
        def test_delayed_flight_carrier(mock_session_commit, mock_session_query):
            # When
            logic.create_top10_delayed_flight_carriers()
            # Then
            mock_session_query.assert_called()
            mock_session_commit.assert_called_once()

    with it.having("delayed minute airport carriers"):
        @it.should('Top 10 delayed minute airport carriers!')
        @patch('logic.session.query')
        @patch('logic.session.commit')
        def test_minute_airport_carriers(mock_session_commit, mock_session_query):
            # When
            logic.create_top10_delayed_minute_airport_carriers()
            # Then
            it.assertEqual(mock_session_query.call_count, 3)
            mock_session_commit.assert_called_once()

    with it.having(' export to csv'):
        @it.should('export to csv')
        @patch('pandas.read_sql')
        @patch('pandas.DataFrame.to_csv')
        def test_export_to_csv(mock_to_csv, mock_read_sql):
            # Given
            mock_read_sql.return_value = pandas.DataFrame
            # When
            logic.export_to_csv('abc')
            # Then
            mock_read_sql.assert_called()
            mock_to_csv.assert_called()

    with it.having(' export All, export to CSV'):
        @it.should('export All')
        @patch('logic.export_to_csv')
        def test_exportAll(mock_export_to_csv):
            logic.export_all()
            it.assertEqual(mock_export_to_csv.call_count, 6)

it.createTests(globals())
