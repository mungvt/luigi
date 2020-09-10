from datetime import datetime

import luigi

import logic


class BaseTask(luigi.Task):
    """
    Output Create a Abstract Class
    """
    date = luigi.DateParameter(default=datetime.now())

    def output(self):
        return logic.create_target(self)

    def execute(self):
        pass

    def run(self):
        cls_name = self.__class__.__name__
        logic.write_log("Start {}. [{}]".format(cls_name, self.date))
        try:
            self.execute()
        except Exception as e:
            logic.write_log("Exception.[{}]".format(str(e)))
            raise
        logic.touch_target(self, cls_name)
        logic.write_log("End {}.".format(cls_name))
        return


class CreateAllTables(BaseTask):
    """
    Output Create all tables which use to prepare for next tasks.
    """
    def execute(self):
        logic.create_all_tables()


class ImportDB(BaseTask):
    """
    Output A database had the data.
    """

    def requires(self):
        return CreateAllTables()

    def execute(self):
        logic.import_data_to_mysql()


class CreateTop10DelayedFlightAirports(BaseTask):
    """
    Output A table in database what lists top 10 delayed flight airports.
    """
    def requires(self):
        return ImportDB()

    def execute(self):
        logic.create_top10_delayed_flight_airports()


class CreateTop10DelayedMinuteAirports(BaseTask):
    """
    Output A table in database what lists top 10 delayed minute airports.
    """
    def requires(self):
        return ImportDB()

    def execute(self):
        logic.create_top10_delayed_minute_airports()


class CreateTop10DelayedFlightCities(BaseTask):
    """
    Output A table in database what lists top 10 delayed flight cities.
    """
    def requires(self):
        return ImportDB()

    def execute(self):
        logic.create_top10_delayed_flight_cities()


class CreateTop10DelayedMinuteAirportCities(BaseTask):
    """
    Output A table in database what lists top 10 delayed minute airport cities.
    """
    def requires(self):
        return CreateTop10DelayedFlightCities()

    def execute(self):
        logic.create_top10_delayed_minute_airport_cities()


class CreateTop10DelayedFlightCarriers(BaseTask):
    """
    Output A table in database what lists top 10 delayed flight carriers.
    """
    def requires(self):
        return ImportDB()

    def execute(self):
        logic.create_top10_delayed_flight_carriers()


class CreateTop10DelayedMinuteAirportCarriers(BaseTask):
    """
    Output A table in database what lists top 10 delayed minute airport carriers.
    """
    def requires(self):
        return CreateTop10DelayedFlightCarriers()

    def execute(self):
        logic.create_top10_delayed_minute_airport_carriers()


class ExportToCSV(BaseTask):
    """
    Output 6 csv files
    """
    def requires(self):
        yield CreateTop10DelayedFlightAirports()
        yield CreateTop10DelayedMinuteAirports()
        yield CreateTop10DelayedMinuteAirportCities()
        yield CreateTop10DelayedMinuteAirportCarriers()

    def execute(self):
        logic.export_all()


class AllTasks(luigi.WrapperTask):
    """
    Output A WrapperTask
    """
    def requires(self):
        return ExportToCSV()
