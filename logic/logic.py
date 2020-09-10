import json
import logging
import os
import re

import luigi
import pandas
from luigi.contrib.mysqldb import MySqlTarget
from sqlalchemy import create_engine, func
from sqlalchemy.orm import aliased, sessionmaker

import config
import models

# Create engine
uri = "mysql+pymysql://{user}:{pw}@{host}/{db}".format(
    user=config.user,
    pw=config.pw,
    host=config.host,
    db=config.db
)
engine = create_engine(uri, echo=False)
# Create session
Session = sessionmaker(bind=engine)
session = Session()


def is_dryrun():
    """
    Return mode
    :rtype bool
    :return: true or false
    """
    return config.DRYRUN


def create_dir(_path):
    """
    Create a folder
    :param string _path: a path to the folder
    :return:
    """
    try:
        direct_path = os.getcwd() + _path
        if not os.path.exists(direct_path):
            os.mkdir(direct_path)
    except OSError:
        exit(1)


def convert_name(class_name):
    """
    Convert from class name to table name
    :param string class_name: a class name
    :rtype string
    :return: a table name or None
    """
    element_name = re.findall('[A-Z][^A-Z]*', class_name)

    if element_name[0] == 'Create':
        tbl_name = ''
        for i in element_name[1:]:
            tbl_name += i.lower() + '_'
        tbl_name = tbl_name[0:len(tbl_name) - 1]
        return tbl_name
    else:
        return None


def create_target(target):
    """
    Create a LocalTarget in DRYRUN mode
    Create a MySqlTarget in normal mode
    :param object target: a instance of the class
    :rtype object
    :return: a luigi Target
    """
    class_name = target.__class__.__name__
    tbl_name = convert_name(class_name)
    update_id = "{}_{}".format(class_name, target.date)
    if is_dryrun():
        return luigi.LocalTarget("dryrun/{}.txt".format(class_name))
    else:
        return MySqlTarget(
            host=config.host,
            database=config.db,
            user=config.user,
            password=config.pw,
            table=tbl_name,
            update_id=update_id
        )


def touch_target(target, cls_name):
    """
    Write into the file in DRYRUN mode
    Touch into the database in normal mode
    :param string cls_name: name of the class
    :param object target: a instance of the class
    :return:
    """
    if is_dryrun():
        with target.output().open('w') as f:
            f.write(
                "{} {}".format(
                    str(target.date),
                    cls_name)
            )
    else:
        target.output().touch()


def write_log(qr):
    """
    Write log Dryrun

    :param qr: query sqlAlchemy
    :return:
    """
    create_dir("/dryrun")
    create_dir("/log")
    logging.basicConfig(
        filename='./log/logfile.log',
        filemode='a',
        format="%(asctime)s [INFO] %(message)s",
        level=logging.DEBUG
    )
    if type(qr) is str:
        logging.info("[DRYRUN]: {}".format(qr))
    else:
        logging.info("[DRYRUN]: SQL query {}".format(str(qr)))


def create_all_tables():
    """
    Create tables in models.py

    :return:
    """
    if is_dryrun():
        write_log("Create all tables")
    else:
        # Drop All Tables
        models.Base.metadata.drop_all(engine)
        # Create All Tables
        models.Base.metadata.create_all(engine)


def read_file_json(file_path):

    """
    Read a json file from file_path

    :param string file_path: path to the json file
    :rtype: list
    :return: a list contain data
    """

    try:
        with open(file_path, 'r') as json_file:
            try:
                json_data = json.load(json_file)
            except ValueError:
                exit(1)

    except OSError:
        exit(1)
    return json_data


def convert_data(json_data):
    """
    Convert name of dict json_data to match with column name in model
    :param list json_data: a list of dicts
    :rtype list
    :return: a list of dicts
    """
    logic_data = []
    for row in json_data:
        record = {
            'airport_code': row['airport']['code'],
            'airport_city': row['airport']['name'].split(",")[0],
            'airport_name': row['airport']['name'].split(",")[1],
            'statistics_flight_cancelled': row['statistics']['flights']['cancelled'],
            'statistics_flight_ontime': row['statistics']['flights']['on time'],
            'statistics_flight_total': row['statistics']['flights']['total'],
            'statistics_flight_delayed': row['statistics']['flights']['delayed'],
            'statistics_flight_diverted': row['statistics']['flights']['diverted'],
            'statistics_delay_late_aircraft': row['statistics']['# of delays'][
                'late aircraft'],
            'statistics_delay_weather': row['statistics']['# of delays']['weather'],
            'statistics_delay_security': row['statistics']['# of delays'][
                'security'],
            'statistics_delay_national_aviation_system': row['statistics'][
                '# of delays']['national aviation system'],
            'statistics_numbe': row['statistics']['# of delays']['carrier'],
            'statistics_minutes_delayed_late_aircraft': row['statistics'][
                'minutes delayed']['late aircraft'],
            'statistics_minutes_delayed_weather': row['statistics']['minutes delayed'][
                'weather'],
            'statistics_minutes_delayed_carrier': row['statistics']['minutes delayed'][
                'carrier'],
            'statistics_minutes_delayed_security': row['statistics'][
                'minutes delayed']['security'],
            'statistics__minr_delays__carrier': row['statistics']['minutes delayed'][
                'total'],
            'statistics_minutes_national_aviation_system': row['statistics'][
                'minutes delayed']['national aviation system'],
            'time_label': row['time']['label'],
            'time_year': row['time']['year'],
            'time_month': row['time']['month'],
            'carrier_code': row['carrier']['code'],
            'carrier_name': row['carrier']['name']
        }
        logic_data.append(record)
    return logic_data


def import_data_to_mysql():
    """
    Import data from airlines.json file into DB

    :return:
    """

    if is_dryrun():
        write_log("Import Database")
    else:
        count = 0
        json_data = read_file_json('./airlines.json')
        data = convert_data(json_data)
        try:
            for record in data:
                count += 1
                airline = models.AirlineModel(**record)
                session.add(airline)
                if count % 10000 == 0:
                    session.commit()
            session.commit()
        except Exception as e:
            logging.exception(e)
            session.rollback()


def create_top10_delayed_flight_airports():
    """
    Select top 10 delayed flight airports from DB and
    save into the DB with table name: top10_delayed_flight_airports

    :return:
    """
    airl = aliased(models.AirlineModel)
    qr = session.query(
        func.sum(
            airl.statistics_flight_delayed
        ).label('flight'),
        airl.airport_code,
        airl.airport_name
    ).group_by(
        airl.airport_code, airl.airport_name
    ).order_by(
        func.sum(
            airl.statistics_flight_delayed
        ).desc()
    ).limit(10)

    if is_dryrun():
        write_log(qr)
    else:
        for i in qr:
            row = models.Top10DelayedFlightAirports(
                airport_code=i.airport_code,
                airport_name=i.airport_name,
                flights=i.flight
            )
            session.add(row)
        session.commit()


def create_top10_delayed_minute_airports():
    """
    Select top 10 delayed minute airports from DB and
    save into the DB with table name: top10_delayed_minute_airports

    :return:
    """
    airl = aliased(models.AirlineModel)
    qr = session.query(
        airl.airport_code,
        airl.airport_name,
        func.sum(
            airl.statistics__minr_delays__carrier
        ).label('min')
    ).group_by(
        airl.airport_code,
        airl.airport_name
    ).order_by(
        func.sum(
            airl.statistics__minr_delays__carrier
        ).desc()
    ).limit(10)

    if is_dryrun():
        write_log(qr)
    else:
        for i in qr:
            row = models.Top10DelayedMinuteAirports(
                airport_code=i.airport_code,
                airport_name=i.airport_name,
                mins=i.min
            )
            session.add(row)
        session.commit()


def create_top10_delayed_flight_cities():
    """
    Select top 10 delayed flight cities from DB and
    save into the DB with table name: top10_delayed_flight_cities

    :return:
    """
    airl = aliased(models.AirlineModel)
    qr = session.query(
        airl.airport_city,
        func.sum(
            airl.statistics_flight_delayed
        ).label('flight')
    ).group_by(
        airl.airport_city
    ).order_by(
        func.sum(
            airl.statistics_flight_delayed
        ).desc()
    ).limit(10)

    if is_dryrun():
        write_log(qr)
    else:
        for i in qr:
            row = models.Top10DelayedFlightCities(
                city=i.airport_city,
                flights=i.flight
            )
            session.add(row)
        session.commit()


def create_top10_delayed_minute_airport_cities():
    """
    Select top 10 delayed minute airport cities from DB and
    save into the DB with table name: top10_delayed_minute_airport_cities

    :return:
    """
    airl = aliased(models.AirlineModel)
    dfc = aliased(models.Top10DelayedFlightCities)

    qr1 = session.query(
        airl.airport_name.label('airport'),
        airl.airport_city.label('city'),
        func.sum(
            airl.statistics__minr_delays__carrier
        ).label('min')
    ).filter(
        airl.airport_city.contains(dfc.city)
    ).group_by(
        airl.airport_city,
        airl.airport_name
    ).order_by(
        func.sum(
            airl.statistics__minr_delays__carrier
        )
    ).subquery()

    qr2 = session.query(
        qr1.c.city.label('city'),
        func.max(
            qr1.c.min
        ).label('top_min')
    ).group_by(
        qr1.c.city
    ).subquery()

    qr3 = session.query(
        qr1.c.city,
        qr1.c.airport,
        qr1.c.min
    ).filter(
        qr1.c.min == qr2.c.top_min
    ).order_by(
        qr1.c.min.desc()
    )

    if is_dryrun():
        write_log(qr3)
    else:
        for i in qr3:
            row = models.Top10DelayedMinuteAirportCities(
                city=i.city,
                airport_name=i.airport,
                mins=i.min
            )
            session.add(row)
        session.commit()


def create_top10_delayed_flight_carriers():
    """
    Select top 10 delayed flight carriers from DB and
    save into the DB with table name: top10_delayed_flight_carriers

    :return:
    """
    airl = aliased(models.AirlineModel)
    qr = session.query(
        airl.carrier_name,
        func.sum(
            airl.statistics_flight_delayed
        ).label('flight')
    ).group_by(
        airl.carrier_name
    ).order_by(
        func.sum(
            airl.statistics_flight_delayed
        ).desc()
    ).limit(10)

    if is_dryrun():
        write_log(qr)
    else:
        for i in qr:
            row = models.Top10DelayedFlightCarriers(
                carrier=i.carrier_name,
                flights=i.flight
            )
            session.add(row)
        session.commit()


def create_top10_delayed_minute_airport_carriers():
    """
    Select top 10 delayed minute airport carriers from DB and
    save into the DB with table name: top10_delayed_minute_airport_carriers

    :return:
    """
    airl = aliased(models.AirlineModel)
    dfca = aliased(models.Top10DelayedFlightCarriers)

    qr1 = session.query(
        airl.airport_name.label('airport'),
        airl.carrier_name.label('carrier'),
        func.sum(
            airl.statistics__minr_delays__carrier
        ).label('min')
    ).filter(
        airl.carrier_name == dfca.carrier
    ).group_by(
        airl.carrier_name,
        airl.airport_name
    ).order_by(
        func.sum(
            airl.statistics__minr_delays__carrier
        )
    ).subquery()

    qr2 = session.query(
        qr1.c.carrier.label('carrier'),
        func.max(
            qr1.c.min
        ).label('top_min')
    ).group_by(
        qr1.c.carrier
    ).subquery()

    qr3 = session.query(
        qr1.c.carrier,
        qr1.c.airport,
        qr1.c.min
    ).filter(
        qr1.c.min == qr2.c.top_min
    ).order_by(
        qr1.c.min.desc()
    ).limit(10)

    if is_dryrun():
        write_log(qr3)
    else:
        for i in qr3:
            row = models.Top10DelayedMinuteAirportCarriers(
                carrier=i.carrier,
                airport=i.airport,
                mins=i.min
            )
            session.add(row)
        session.commit()


def export_to_csv(tbl_name):
    """
    Export a table to csv file

    :param string tbl_name: name of the table
    :return:
    """

    if is_dryrun():
        write_log("Export {} to {}.csv".format(tbl_name, tbl_name))
    else:
        data_frame = pandas.read_sql("SELECT * FROM {}".format(tbl_name), engine)
        data_frame.to_csv(
            "csv/{}.csv".format(tbl_name),
            index=False,
            index_label=False
        )


def export_all():
    """
    Export all tables

    :return:
    """
    create_dir("/csv")
    export_to_csv('top10_delayed_flight_airports')
    export_to_csv('top10_delayed_minute_airports')
    export_to_csv('top10_delayed_flight_cities')
    export_to_csv('top10_delayed_minute_airport_cities')
    export_to_csv('top10_delayed_flight_carriers')
    export_to_csv('top10_delayed_minute_airport_carriers')
