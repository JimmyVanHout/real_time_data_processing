import datetime
import json
import sqlite3
import typing

DATABASE_FILE_NAME = "data.db"

def initialize_database(max_num_members: int = 20) -> None:
    """
    Initializes a database with the tables studios and members.

    max_num_members: The maximum number of members a class can hold.
    """
    connection = sqlite3.connect(DATABASE_FILE_NAME)
    cursor = connection.cursor()
    cursor.execute(
        """
        CREATE TABLE studios (
            id int PRIMARY KEY,
            start_time datetime,
            {ms}
        )
        """
        .format(ms=",\n".join(["member_" + str(i) + "_id int REFERENCES members" for i in range(1, max_num_members + 1)]))
    )
    cursor.execute(
        """
        CREATE TABLE members (
            id int PRIMARY KEY,
            latest_time_stamp datetime,
            count int,
            avg_hr float,
            speed float,
            distance float
        )
        """
    )

def process_payload(payload: str) -> None:
    """
    Receives a payload consisting of a JSON object encoded as a string (see the specification above) and adds the encoded data to the database. It is assumed that if a studio exists in the table studios, then all of its members also exist in the table members. For speed, it is also assumed that additional members will not be added during a class: that is, once the studio is add to the studios table and the members are added to the members table (or they may already be present), no members will be added in the future during the current class.

    payload: A JSON object encoded as a string (see the specification above).
    """
    data = json.loads(payload)
    studio_id = data["studio_id"]
    time_stamp = datetime.datetime.fromisoformat(data["time_stamp"])
    members_data = {}
    for member_data in data["members_data"]:
        member_id = member_data["member_id"]
        heart_rate = member_data["heart_rate"]
        speed = member_data["speed"]
        distance = member_data["distance"]
        members_data[member_id] = {
            "latest_time_stamp": time_stamp,
            "heart_rate": heart_rate,
            "speed": speed,
            "distance": distance,
        }
    connection = sqlite3.connect(DATABASE_FILE_NAME)
    cursor = connection.cursor()
    retrieved_studio_data = cursor.execute(
        """
        SELECT id
        FROM studios
        WHERE id = ?;
        """,
        (studio_id,)
    ).fetchone()

    # check if studio is in database
    if retrieved_studio_data:
        # update member data in database
        cursor.executemany(
            """
            UPDATE members
            SET latest_time_stamp = ?, count = count + 1, avg_hr = avg_hr + (? - avg_hr) / (count + 1), speed = ?, distance = ?
            WHERE id = ?;
            """,
            [(md["latest_time_stamp"], md["heart_rate"], md["speed"], md["distance"], id) for id, md in members_data.items()]
        )
    else:
        # studio not in database, so add studio, start time, and members taking class at studio to database
        cursor.execute(
            """
            INSERT INTO studios (id, start_time, {ms}) VALUES (?, ?, {qs});
            """
            .format(ms=", ".join(["member_" + str(i) + "_id" for i in range(1, len(members_data) + 1)]), qs=", ".join(["?"] * len(members_data))),
            (studio_id, datetime.datetime.now().isoformat(), *tuple(members_data.keys()))
        )

        # add member data to database
        cursor.executemany(
            """
            INSERT INTO members (id, latest_time_stamp, count, avg_hr, speed, distance) VALUES (?, ?, 1, ?, ?, ?);
            """,
            [(id, md["latest_time_stamp"], md["heart_rate"], md["speed"], md["distance"]) for id, md in members_data.items()]
        )
    connection.commit()
    cursor.close()
    connection.close()

def get_class_summary(studio_id: int) -> str:
    """
    Returns a summary of members' statistics for a class held at the studio with the specified ID.

    studio_id: The ID of the studio for which to receive the class summary.
    """
    members_data = {}
    connection = sqlite3.connect(DATABASE_FILE_NAME)
    cursor = connection.cursor()
    retrieved_studio_data = cursor.execute(
        """
        SELECT *
        FROM studios
        WHERE id = ?;
        """,
        (studio_id,)
    ).fetchone()

    # check if studio is in database
    if retrieved_studio_data:
        start_time = datetime.datetime.fromisoformat(retrieved_studio_data[1])

        # get data of members who took class at studio
        retrieved_members_data = cursor.execute(
            """
            SELECT id, latest_time_stamp, count, avg_hr, distance
            FROM members
            WHERE id = ? OR id = ? OR id = ?;
            """,
            tuple(id for id in retrieved_studio_data[2:5])
        ).fetchall()
        if retrieved_members_data:
            for retrieved_member_data in retrieved_members_data:
                id, latest_time_stamp, count, avg_hr, distance = retrieved_member_data
                latest_time_stamp = datetime.datetime.fromisoformat(latest_time_stamp)
                avg_speed = distance / ((latest_time_stamp - start_time) / datetime.timedelta(hours=1))
                members_data[id] = {
                    "avg_hr": round(avg_hr),
                    "avg_speed": round(avg_speed, 1),
                    "distance": round(distance, 2),
                }
    members_data_json = json.dumps(members_data)
    cursor.close()
    connection.close()
    return members_data_json

def test_process_payload() -> bool:
    connection = sqlite3.connect(DATABASE_FILE_NAME)
    cursor = connection.cursor()
    retrieved_studio_data = cursor.execute(
    """
        SELECT *
        FROM studios;
    """
    ).fetchone()
    if retrieved_studio_data:
        raise Exception("Table studios in database must be empty.")
    retrieved_member_data = cursor.execute(
    """
        SELECT *
        FROM members;
    """
    ).fetchone()
    if retrieved_member_data:
        raise Exception("Table members in database must be empty.")
    s = """
        {
            "studio_id": 3,
            "time_stamp": "2022-07-26T12:50:37.944260",
            "members_data": [
                {
                    "member_id": 5,
                    "heart_rate": 162,
                    "speed": 3.4,
                    "distance": 0.88
                },
                {
                    "member_id": 18,
                    "heart_rate": 188,
                    "speed": 3.9,
                    "distance": 0.15
                },
                {
                    "member_id": 16,
                    "heart_rate": 174,
                    "speed": 5.8,
                    "distance": 1.15
                }
            ]
        }
    """
    t = """
        {
            "studio_id": 3,
            "time_stamp": "2022-07-26T12:53:39.366260",
            "members_data": [
                {
                    "member_id": 5,
                    "heart_rate": 160,
                    "speed": 3.5,
                    "distance": 1.0
                },
                {
                    "member_id": 16,
                    "heart_rate": 170,
                    "speed": 5.8,
                    "distance": 1.30
                },
                {
                    "member_id": 18,
                    "heart_rate": 196,
                    "speed": 4.0,
                    "distance": 0.72
                }
            ]
        }
    """
    process_payload(s)
    process_payload(t)
    retrieved_studio_data = cursor.execute(
    """
        SELECT id, start_time, member_1_id, member_2_id, member_3_id
        FROM studios
        WHERE id = 3;
    """
    ).fetchone()
    assert retrieved_studio_data is not None, "Studio with ID 3 is not in table studios."
    assert len(retrieved_studio_data) == 5, "Expected 3 members in table studio, received {}".format(len(retrieved_studio_data))
    assert retrieved_studio_data[2] == 5, "Expected member ID 5 in table studio, received {}".format(retrieved_studio_data[2])
    assert retrieved_studio_data[3] == 18, "Expected member ID 18 in table studio, received {}".format(retrieved_studio_data[3])
    assert retrieved_studio_data[4] == 16, "Expected member ID 16 in table studio, received {}".format(retrieved_studio_data[4])
    retrieved_members_data = cursor.execute(
    """
        SELECT id, latest_time_stamp, count, avg_hr, speed, distance
        FROM members
        WHERE id = ? OR id = ? OR id = ?
        ORDER BY id;
    """,
    (5, 18, 16)
    ).fetchall()
    assert len(retrieved_members_data) == 3, "Expected 3 members in table members, received {}".format(len(retrieved_studio_data))
    expected_members_data = {
        5: ["2022-07-26 12:53:39.366260", 2, 161.00, 3.5, 1.0],
        16: ["2022-07-26 12:53:39.366260", 2, 172.00, 5.8, 1.30],
        18: ["2022-07-26 12:53:39.366260", 2, 192.00, 4.0, 0.72],
    }
    for i in range(len(retrieved_members_data)):
        id = retrieved_members_data[i][0]

        # test for correct time stamp
        assert retrieved_members_data[i][1] == expected_members_data[id][0], "Expected time stamp {expected} for member with ID {id}, received {received}.".format(expected=expected_members_data[id][0], id=id, received=retrieved_members_data[i][1])

        # test for correct count
        assert retrieved_members_data[i][2] == expected_members_data[id][1], "Expected count {expected} for member with ID {id}, received {received}.".format(expected=expected_members_data[id][1], id=id, received=retrieved_members_data[i][2])

        # test for correct average heart rate
        assert round(retrieved_members_data[i][3], 2) == expected_members_data[id][2], "Expected average heart rate {expected} for member with ID {id}, received {received}.".format(expected=expected_members_data[id][2], id=id, received=round(retrieved_members_data[i][3], 2))

        # test for correct speed
        assert retrieved_members_data[i][4] == expected_members_data[id][3], "Expected speed {expected} for member with ID {id}, received {received}.".format(expected=expected_members_data[id][3], id=id, received=retrieved_members_data[i][4])

        # test for correct distance
        assert retrieved_members_data[i][5] == expected_members_data[id][4], "Expected distance {expected} for member with ID {id}, received {received}.".format(expected=expected_members_data[id][4], id=id, received=retrieved_members_data[i][5])
    cursor.close()
    connection.close()
    return True

def test_get_class_summary() -> bool:
    connection = sqlite3.connect(DATABASE_FILE_NAME)
    cursor = connection.cursor()
    retrieved_studio_data = cursor.execute(
    """
        SELECT *
        FROM studios;
    """
    ).fetchone()
    if retrieved_studio_data:
        raise Exception("Table studios in database must be empty.")
    retrieved_member_data = cursor.execute(
    """
        SELECT *
        FROM members;
    """
    ).fetchone()
    if retrieved_member_data:
        raise Exception("Table members in database must be empty.")
    cursor.execute(
        """
        INSERT INTO studios (id, start_time, member_1_id, member_2_id, member_3_id) VALUES (3, "2022-07-26T12:50:37.944260", 5, 16, 18);
        """
    )
    cursor.executemany(
        """
        INSERT INTO members (id, latest_time_stamp, count, avg_hr, speed, distance) VALUES (?, ?, ?, ?, ?, ?);
        """,
        [(5, "2022-07-26 12:53:39.366260", 2, 161.00, 3.5, 1.0), (16, "2022-07-26 12:53:39.366260", 2, 172.00, 5.8, 1.30), (18, "2022-07-26 12:53:39.366260", 2, 192.00, 4.0, 0.72)]
    )
    connection.commit()
    cursor.close()
    connection.close()
    expected = {
        5: {
            "avg_hr": 161,
            "avg_speed": 19.8,
            "distance": 1.00,
        },
        16: {
            "avg_hr": 172,
            "avg_speed": 25.8,
            "distance": 1.30,
        },
        18: {
            "avg_hr": 192,
            "avg_speed": 14.3,
            "distance": 0.72,
        },
    }
    assert get_class_summary(3) == json.dumps(expected), "Error in class summary."
    return True
