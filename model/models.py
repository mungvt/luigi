from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


# Create model
class AirlineModel(Base):
    __tablename__ = "airline"
    id = Column(
        Integer,
        autoincrement=True,
        primary_key=True,
        nullable=False
    )
    airport_code = Column(
        String(4),
        nullable=False
    )
    airport_city = Column(
        String(100),
        nullable=False
    )
    airport_name = Column(
        String(200),
        nullable=False
    )
    statistics_flight_cancelled = Column(
        Integer,
        nullable=False
    )
    statistics_flight_ontime = Column(
        Integer,
        nullable=False
    )
    statistics_flight_total = Column(
        Integer,
        nullable=False
    )
    statistics_flight_delayed = Column(
        Integer,
        nullable=False
    )
    statistics_flight_diverted = Column(
        Integer,
        nullable=False
    )
    statistics_delay_late_aircraft = Column(
        Integer,
        nullable=False
    )
    statistics_delay_weather = Column(
        Integer,
        nullable=False
    )
    statistics_delay_security = Column(
        Integer,
        nullable=False
    )
    statistics_delay_national_aviation_system = Column(
        Integer,
        nullable=False
    )
    statistics_numbe = Column(
        Integer,
        nullable=False
    )
    statistics_minutes_delayed_late_aircraft = Column(
        Integer,
        nullable=False
    )
    statistics_minutes_delayed_weather = Column(
        Integer,
        nullable=False
    )
    statistics_minutes_delayed_carrier = Column(
        Integer,
        nullable=False
    )
    statistics_minutes_delayed_security = Column(
        Integer,
        nullable=False
    )
    statistics__minr_delays__carrier = Column(
        Integer,
        nullable=False
    )
    statistics_minutes_national_aviation_system = Column(
        Integer,
        nullable=False
    )
    time_label = Column(
        String(10),
        nullable=False
    )
    time_year = Column(
        Integer,
        nullable=False
    )
    time_month = Column(
        Integer,
        nullable=False
    )
    carrier_code = Column(
        String(3),
        nullable=False
    )
    carrier_name = Column(
        String(100),
        nullable=False
    )


class Top10DelayedFlightAirports(Base):
    __tablename__ = "top10_delayed_flight_airports"
    No = Column(
        Integer,
        autoincrement=True,
        primary_key=True,
        nullable=False
    )
    airport_code = Column(
        String(4),
        nullable=False
    )
    airport_name = Column(
        String(200),
        nullable=False
    )
    flights = Column(
        Integer,
        nullable=False
    )


class Top10DelayedMinuteAirports(Base):
    __tablename__ = "top10_delayed_minute_airports"
    No = Column(
        Integer,
        autoincrement=True,
        primary_key=True,
        nullable=False
    )
    airport_code = Column(
        String(4),
        nullable=False
    )
    airport_name = Column(
        String(200),
        nullable=False
    )
    mins = Column(
        Integer,
        nullable=False
    )


class Top10DelayedFlightCities(Base):
    __tablename__ = "top10_delayed_flight_cities"
    No = Column(
        Integer,
        autoincrement=True,
        primary_key=True,
        nullable=False
    )
    city = Column(
        String(100),
        nullable=False
    )
    flights = Column(
        Integer,
        nullable=False
    )


class Top10DelayedMinuteAirportCities(Base):
    __tablename__ = "top10_delayed_minute_airport_cities"
    No = Column(
        Integer,
        autoincrement=True,
        primary_key=True,
        nullable=False
    )
    city = Column(
        String(100),
        nullable=False
    )
    airport_name = Column(
        String(200),
        nullable=False
    )
    mins = Column(
        Integer,
        nullable=False
    )


class Top10DelayedFlightCarriers(Base):
    __tablename__ = "top10_delayed_flight_carriers"
    No = Column(
        Integer,
        autoincrement=True,
        primary_key=True,
        nullable=False
    )
    carrier = Column(
        String(200),
        nullable=False
    )
    flights = Column(
        Integer,
        nullable=False
    )


class Top10DelayedMinuteAirportCarriers(Base):
    __tablename__ = "top10_delayed_minute_airport_carriers"
    No = Column(
        Integer,
        autoincrement=True,
        primary_key=True,
        nullable=False
    )
    carrier = Column(
        String(200),
        nullable=False
    )
    airport = Column(
        String(200),
        nullable=False
    )
    mins = Column(
        Integer,
        nullable=False
    )
