# Real Time Data Processing

## About

This program demonstrates the use of API calls to serverless functions to update group fitness members' statistics in real time. The statistics include average heart rate, cumulative distance run on a treadmill, and average speed on the treadmill. The program is designed to be run on a distributed cloud architecture, such as AWS Lambda, which scales to allow multiple studios to call the API at the same time. The program uses an SQLite database for simplicity, so concurrent access would not be feasible unless a more scalable database such as PostgreSQL were substituted.

## Installation

Clone the repository from [GitHub](https://github.com/JimmyVanHout/real_time_data_processing):

```
git clone https://github.com/JimmyVanHout/real_time_data_processing.git
```

## Set Up

Before running the program, either run the provided helper function `initialize_database` or manually create an SQLite database with the following tables and set the global variable `DATABASE_FILE_NAME` to the file name of the database:

`studios`:

key | type
--- | ---
`id` | `int`
`start_time` | `datetime`
`member_id_1` | `int`
`member_id_2` | `int`
... | ...

`members`:

key | type
--- | ---
`id` | `int`
`latest_time_stamp` | `datetime`
`count` | `int`
`avg_hr` | `float`
`speed` | `float`
`distance` | `float`

## Usage

To update the database with data from a payload, run:

```
process_payload(payload)
```

where `payload` is the payload. See the [payload specification](#payload-specification) below.

To receive a summary of members' up-to-date statistics for a specific studio during or after a class, run:

```
get_class_summary(studio_id)
```

where `studio_id` is the ID of the studio for which to obtain the class summary. See the [class summary specification](#class-summary-specification) below.

## Testing

To test `process_payload`, run:

```
test_process_payload()
```

To test `get_class_summary`, run:

```
test_get_class_summary()
```

Note that the tables `studios` and `members` in the database *must* be empty to run these tests.

## Payload Specification

The program expects the payload to be a JSON object encoded as a string. The object itself should have the following specifications:

```
{
    "studio_id": <studio_id>,
    "time_stamp": <time_stamp>,
    "members_data": [
        {
            "member_id": <member_id>,
            "heart_rate": <heart_rate>,
            "speed": <speed>,
            "distance": <distance>
        },
        ...
    ]
}
```

`<studio_id>`: The unique ID of the studio where the class is taking place.
`<time_stamp>`: An [ISO 8601](https://www.iso.org/iso-8601-date-and-time-format.html)-formatted time stamp.
`<member_id>`: The unique ID of a member taking the class.
`<heart_rate>`: The current heart rate of the member in beats per minute.
`<speed>`: The programmed speed of the treadmill in miles per hour.
`<distance>`: The cumulative distance reported by the treadmill.

## Class Summary Specification

```
{
    <member_id>: {
        "avg_hr": <avg_hr>,
        "avg_speed": <avg_speed>,
        "distance": <distance>
    },
    ...
}
```

`<member_id>`: The unique ID of a member taking the class.
`<avg_hr>`: The average heart rate of the member in beats per minute.
`<avg_speed>`: The average speed of the member from the class start time until the last received time stamp, in miles per hour.
`<distance>`: The cumulative distance reported by the treadmill.
