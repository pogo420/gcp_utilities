from random import randint
from google.cloud import bigquery
from time import sleep

delay = 15


def get_rand():
    return randint(0, 10)


def bq_inser():
    client = bigquery.Client(project="sublime-mission-251813")
    query = "insert into `practice_data_1.sensor_data` values(\
    current_timestamp(), {0})".format(get_rand())
    client.query(query)
    print("executing query:", query)


def main():
    print("Sensor ON")
    while 1:
        bq_inser()
        sleep(delay)
    print("Sensor OFF")


if __name__ == "__main__":
    main()
