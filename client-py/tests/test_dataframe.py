from iotdb.Session import Session
from iotdb.TestContainer import IoTDBContainer


def test_simple_query():
    with IoTDBContainer("apache/iotdb:0.11.2") as db:
        db: IoTDBContainer
        session = Session(db.get_container_host_ip(), db.get_exposed_port(6667))
        session.open(False)

        # Write data
        session.insert_str_record("device", 123, "pressure", "15.0")

        # Read
        session_data_set = session.execute_query_statement("SELECT * FROM root.*")
        df = session_data_set.todf()

        session.close()

    assert df == []