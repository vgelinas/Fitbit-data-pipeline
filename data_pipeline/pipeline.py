"""
Data pipeline classes.
"""
from fitbit_api import Fitbit
from sqlalchemy import func
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import sessionmaker
import contextlib
import datetime
import db_connection
import db_tables
import logging
import numpy as np
import pandas as pd
import requests
import time


class Pipeline:

    def __init__(self, seconds_between_calls=24, verbose=False):
        self.seconds_between_calls = seconds_between_calls
        self.verbose = verbose
        self.engine = db_connection.create_engine()

        # Add session to handle talking to database.
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

        # Add fitbit api wrapper instance.
        self.fitbit = Fitbit(self.session,
                             self.seconds_between_calls,
                             self.verbose)

        # Pipeline components:
        # - Loader fetches web API data;
        self.loader = Loader(self.session, self.fitbit)

        # Log info in a monthly txt file under project_path/logs.
        logfile = ("/absolute/path/to/project/folder/"
                   "/logs/pipeline_{month}.txt")
        this_month = datetime.date.today().strftime("%Y%m")

        # This should be handled through a logger attribute,
        # but I don't understand logger objects or their hierarchy yet.
        # TODO: Reorganize this.
        logging.basicConfig(filename=logfile.format(month=this_month),
                            filemode='a',
                            format='%(message)s'
                            )

    def run(self):
        try:
            self.loader.run()
            self.transformer.run()

        except requests.exceptions.RequestException as e:
            if self.verbose:
                print(e)
                print("Sleeping 5 minutes then trying again.")

            # Log time and exception raised.
            time_now = datetime.datetime.now().strftime("%H:%M:%S %h %d, %Y")
            logging.error(
                    "{time} - {error}".format(time=time_now, error=e))

            # Sleep 5min then retry.
            time.sleep(300)
            self.run()


class Loader:

    def __init__(self, session, fitbit):
        self.session = session
        self.fitbit = fitbit
        self.parser = ResponseParser()

        # General config data to help load & update tables.
        # Store API endpoint urls and which database tables 
        # to insert the result into. 
        self._api_to_database_pathway_data = {
            "activities": {
                "api_endpoint_url": ("https://api.fitbit.com/1/user/-/"
                                     "activities/date/{date}.json"),
                "db_tables": {
                    "Activities": db_tables.Activities,
                    "ActivitiesDailySummary": db_tables.ActivitiesDailySummary
                    },
            },
            "steps": {
                "api_endpoint_url": ("https://api.fitbit.com/1/user/-/"
                                     "activities/steps/date/{date}/1d.json"),
                "db_tables": {
                   "ActivitiesStepsIntraday": db_tables.ActivitiesStepsIntraday
                },
            },
            "heart_rate": {
                "api_endpoint_url": ("https://api.fitbit.com/1/user/-/"
                                     "activities/heart/date/{date}/1d.json"),
                "db_tables": {
                    "HeartRateIntraday": db_tables.HeartRateIntraday
                },
            },
            "sleep": {
                "api_endpoint_url": ("https://api.fitbit.com/1.2/user/-/"
                                     "sleep/date/{date}.json"),
                "db_tables": {
                    "SleepDailySummary": db_tables.SleepDailySummary,
                    "SleepIntraday": db_tables.SleepIntraday
                },
            },
        }

    def run(self):

        # Fetch updated data from each api endpoint currently handled.
        api_endpoints = self._api_to_database_pathway_data.keys()

        for endpoint_name in api_endpoints:
            self._update_database_from_api_endpoint(endpoint_name)

    def _update_database_from_api_endpoint(self, endpoint_name):

        # First, we need to know how the data will tread for that endpoint,
        # from a given endpoint url to possibly multiple database tables.
        pathway_data = self._api_to_database_pathway_data[endpoint_name]

        url = pathway_data["api_endpoint_url"]   # fstring with {date} field
        tables_dict = pathway_data["db_tables"]  # names and ORM table refs

        # Get date range from time of last update (or user start date if empty).
        query_dates = self._get_update_date_range_from_tables(tables_dict)

        for date in query_dates: 

            # Fetch response for that day.
            date_string = date.strftime("%Y-%m-%d")
            response = self.fitbit.get_resource(url.format(date=date_string))
            response = response.json()  # TODO (Future): Want Fitbit to handle this?

            # Treat response, returning a dict of (tablename, df) pairs.
            df_dict = self._parse_response(endpoint_name, response, date)

            # Insert each df into the db, updating current date's values if any.
            for tablename in df_dict: 

                df = df_dict[tablename] 
                table = tables_dict[tablename] 

                self._insert_dataframe_in_table(df, table, date) 

    def _get_update_date_range_from_tables(self, tables_dict):

        # End points for our date range.
        start_date = None
        end_date = datetime.date.today()

        # Each table needs to be updated from its maximal date to today;
        # we'll find the earliest such date overall.
        for table_name in tables_dict:
            table = tables_dict[table_name]

            # Check if non-empty first, then fetch last available date.
            if self.session.query(table).first():
                last_date = self.session.query(func.max(table.date)).scalar()
                self.session.commit()

            # If empty, return user start date.
            else:
                row = self.session.query(db_tables.FitbitUserInfo).first()
                self.session.commit()
                last_date = row.start_date

            # Replace start_date if empty, or if an earlier date is found.
            if not start_date or last_date < start_date:
                start_date = last_date

        # Due to sync issues, we'll start the update back from a day prior.
        # Some API endpoints serve default value data (such as 0) until they
        # get fresh data from the phone/fitbit bracelet, so we need to go back
        # a bit to insure accurate data.
        padding_day = datetime.timedelta(days=1)

        # We'll need the user start date to make sure our range is valid.
        row = self.session.query(db_tables.FitbitUserInfo).first()
        self.session.commit()
        user_start_date = row.start_date

        # Start the update a day prior, if possible.
        if user_start_date < start_date:
            start_date -= padding_day

        date_range = pd.date_range(start=start_date, end=end_date)
        return date_range

    def _parse_response(self, endpoint_name, response, date):

        if endpoint_name == "activities":
            return self.parser.parse_activities_response(response, date)

        if endpoint_name == "steps":
            return self.parser.parse_steps_response(response, date)

        if endpoint_name == "heart_rate":
            return self.parser.parse_heart_rate_response(response, date)

        if endpoint_name == "sleep":
            return self.parser.parse_sleep_response(response, date)

        else:
            raise Exception(
                "Endpoint name has no corresponding parse_response method.")

    def _insert_dataframe_in_table(self, dataframe, table, date):

        if dataframe is None:
            return

        # Input validation: we turn all nan to None, which are inserted as null.
        df = dataframe.replace([np.nan], [None])

        # Set primary key as its own column, since we receive it as index.
        primary_key = inspect(table).primary_key[0].name
        df[primary_key] = df.index

        # We'll split the dataframe into two parts, based on which
        # primary keys already present in the database for that date.
        current_keys = self._get_primary_keys_from_table_on_date(table, date)
        df_known_keys = df[df[primary_key].isin(current_keys)]
        df_unknown_keys = df[~df[primary_key].isin(current_keys)]

        # We'll update the data for known keys using delete-write pattern.
        current_rows = df_known_keys.to_dict("records")
        for row in current_rows:

            old_entry = self.session.query(table).get(row[primary_key])
            self.session.delete(old_entry)

            new_entry = table(**row)
            self.session.add(new_entry)

            self.session.commit()

        # Then we insert the new data.
        new_rows = df_unknown_keys.to_dict("records")  
        for row in new_rows:
            entry = table(**row)
            self.session.add(entry)
            self.session.commit()

    def _get_primary_keys_from_table_on_date(self, table, date):

        # Find the primary key's name.
        primary_key = inspect(table).primary_key[0].name

        # Get all values for that date.
        result = self.session.query(table).filter(table.date == date)

        if result:
            db_keys = [getattr(row, primary_key) for row in result]
            return db_keys

        # If no values then return empty list.
        return []


class ResponseParser:

    def __init__(self):
        pass

    def parse_activities_response(self, response, date):

        # First, define template types for the dataframes to be extracted.
        # We'll construct the response dataframes from these templates,
        # and use them for type validation and type conversion.
        activities_types = {
            "logId": "int64",
            "activityId": "int64",
            "activityParentId": "int64",
            "activityParentName": "str",
            "name": "str",
            "description": "str",
            "hasStartTime": "bool",
            "isFavorite": "bool",
            "hasActiveZoneMinutes": "bool",
            "date": "datetime64[ns]",
            "startDateTime": "datetime64[ns]",
            "endDateTime": "datetime64[ns]",
            "durationMinutes": "int64",
            "steps": "int64",
            "calories": "int64"
        }
        summary_types = {
            "date": "datetime64[ns]",
            "activeScore": "int64",
            "activityCalories": "int64",
            "caloriesBMR": "int64",
            "caloriesOut": "int64",
            "marginalCalories": "int64",
            "sedentaryMinutes": "int64",
            "lightlyActiveMinutes": "int64",
            "fairlyActiveMinutes": "int64",
            "veryActiveMinutes": "int64",
            "restingHeartRate": "int64",
            "steps": "int64"
        }

        # 1. Build activities dataframe:
        # First, we extract the relevant data (i.e. subdicts) and parse it into
        # a first dataframe. This provides a first test of the response format.
        df_response = None
        try:  
            activities = response["activities"]
            if activities:
                df_response = pd.DataFrame(activities)

        except:  # Log bad response format (parse failure).
            pass  # TODO (Future): add logging here.

        # We construct our dataframe from this one, adding data validation.
        df_activities = None
        if df_response is not None:

            # Initialise null dataframe with appropriate shape and columns.
            num_rows = len(df_response.index)
            df_activities = pd.DataFrame(data=None, index=range(num_rows))

            for column in activities_types.keys():
                df_activities[column] = None

            # Fill in columns, handling type checks and conversions:
            # First, some columns are extracted directly, adding type checks.
            for column in ["logId",
                           "activityId",
                           "activityParentId",
                           "activityParentName",
                           "name",
                           "description",
                           "hasStartTime",
                           "isFavorite",
                           "hasActiveZoneMinutes",
                           "steps",
                           "calories"]:

                with contextlib.suppress(KeyError, TypeError):
                    df_activities[column] = df_response[column].astype(
                                                    activities_types[column])

            # The following columns need additional transformation:
            # startDateTime, duration, endDateTime
            with contextlib.suppress(KeyError, TypeError):  # startDateTime
                new_col = df_response["startDate"]+" "+df_response["startTime"]
                df_activities["startDateTime"] = pd.to_datetime(new_col)

            with contextlib.suppress(KeyError, TypeError):  # duration
                # convert duration column from millisec to minutes
                millisecs_in_a_minute = 60000
                column = "durationMinutes"
                df_activities[column] = df_response["duration"]
                df_activities[column] /= millisecs_in_a_minute
                df_activities[column] = df_activities[column].astype(
                                                    activities_types[column])

            with contextlib.suppress(KeyError, TypeError):  # endDateTime
                timedeltas = df_activities["durationMinutes"].apply(
                                    lambda x: datetime.timedelta(minutes=x))

                df_activities["endDateTime"] = df_activities["startDateTime"]
                df_activities["endDateTime"] += timedeltas

            # Next, we datestamp the dataframe.
            df_activities["date"] = date

            # Finally we pass the primary key as index, for standardization.
            df_activities.set_index("logId", inplace=True)

        # 2. Build summary dataframe:
        # Similarly, we first extract relevant data into a first dataframe,
        # then build our dataframe from it with a layer of validation. 
        df_response = None
        try: 
            summary = response["summary"]
            if summary:
                # delete all sub-dicts to get a single dict
                del summary["distances"]
                del summary["heartRateZones"]

                df_response = pd.DataFrame(summary, index=[0])

        except:  # Log bad response format (parse failure).
            pass  # TODO (Future): add logging here.

        # Construct our dataframe, converting the response dataframe. 
        df_summary = None
        if df_response is not None:


            # Initialise null dataframe with appropriate shape and columns.
            num_rows = len(df_response.index)
            df_summary = pd.DataFrame(data=None, index=range(num_rows))

            for column in summary_types.keys():
                df_summary[column] = None

            # Fill in columns, handling type checks and conversions.
            # All columns are extracted directly, adding type checks.
            for column in ["activeScore",
                           "activityCalories",
                           "caloriesBMR",
                           "caloriesOut",
                           "marginalCalories",
                           "sedentaryMinutes",
                           "lightlyActiveMinutes",
                           "fairlyActiveMinutes",
                           "veryActiveMinutes",
                           "restingHeartRate",
                           "steps"]:

                with contextlib.suppress(KeyError, TypeError):
                    df_summary[column] = df_response[column].astype(
                                                    summary_types[column])

            # Next, we datestamp the dataframe.
            df_summary["date"] = date

            # Finally we pass the primary key as index, for standardization.
            df_summary.set_index("date", inplace=True)

        # 3. Format the return object as a dict: 
        # We pair each df with the name of its intended table as key. 
        df_dict = {
            "Activities": df_activities, 
            "ActivitiesDailySummary": df_summary 
        }

        return df_dict

    def parse_steps_response(self, response, date):

        # First, define a template type for the dataframe to be extracted.
        # We'll construct the response dataframe from this template,
        # and we'll use it for type validation and type conversion. 
        steps_types = {
            "date": "datetime64[ns]",
            "time": "datetime64[ns]",
            "num_steps": "int64"
        }

        # 1. Build steps dataframe:
        # First, we extract the relevant data (i.e. subdicts) and parse it into
        # first dataframe. This provides a first test of the response format.
        df_response = None
        try:  
            steps_intraday = response["activities-steps-intraday"]["dataset"]
            if steps_intraday:
                df_response = pd.DataFrame(steps_intraday)

        except:  # Log bad response format.
            pass  # TODO (Future): add logging here.

        # We construct our dataframe from this one, adding data validation.
        df_steps = None
        if df_response is not None:

            # Initialise null dataframe with appropriate shape and columns.
            num_rows = len(df_response.index)
            df_steps = pd.DataFrame(data=None, index=range(num_rows))

            for column in steps_types.keys():
                df_steps[column] = None

            # Fill in columns, handling type checks and conversions.
            with contextlib.suppress(KeyError, TypeError):  # time
                # The time column in response is string in format hh:mm:ss
                # we append the yyyy-mm-dd part of the date to it
                # then convert to datetime
                date_string = date.strftime("%Y-%m-%d")
                df_steps["time"] = date_string + " " + df_response["time"]
                df_steps["time"] = df_steps["time"].astype(steps_types["time"])

            with contextlib.suppress(KeyError, TypeError):  # num_steps
                df_steps["num_steps"] = df_response["value"].astype(
                                                    steps_types["num_steps"])

            # Next, we datestamp the dataframe.
            df_steps["date"] = date

            # Finally we pass the primary key as index, for standardization.
            df_steps.set_index("time", inplace=True)

        # 2. Format the return object as a dict: 
        # We pair the df with the name of its intended table as key.
        df_dict = {
            "ActivitiesStepsIntraday": df_steps 
        }

        return df_dict

    def parse_heart_rate_response(self, response, date):

        # First, define a template type for the dataframe to be extracted.
        # We'll construct the response dataframe from this template,
        # and we'll use it for type validation and type conversion. 
        heart_types = {
            "date": "datetime64[ns]",
            "time": "datetime64[ns]",
            "bpm": "int64"
        }

        # 1. Build heart rate dataframe:
        # First, we extract the relevant data (i.e. subdicts) and parse it into
        # first dataframe. This provides a first test of the response format.
        df_response = None
        try:
            heart_intraday = response["activities-heart-intraday"]["dataset"]
            if heart_intraday:
                df_response = pd.DataFrame(heart_intraday)

        except:  # Log bad response format.
            pass  # TODO (Future): add logging here.

        # We construct our dataframe from this one, adding data validation.
        df_heart = None
        if df_response is not None:

            # Initialise null dataframe with appropriate shape and columns.
            num_rows = len(df_response.index)
            df_heart = pd.DataFrame(data=None, index=range(num_rows))

            for column in heart_types.keys():
                df_heart[column] = None

            # Fill in columns, handling type checks and conversions.
            with contextlib.suppress(KeyError, TypeError):  # time
                # The time column in response is string in format hh:mm:ss.
                # We append the yyyy-mm-dd part of the date to it,
                # then convert to datetime.
                date_string = date.strftime("%Y-%m-%d")
                df_heart["time"] = date_string + " " + df_response["time"]
                df_heart["time"] = df_heart["time"].astype(heart_types["time"])

            with contextlib.suppress(KeyError, TypeError):  # bpm
                df_heart["bpm"] = df_response["value"].astype(
                                                            heart_types["bpm"])

            # Next, we datestamp the dataframe.
            df_heart["date"] = date

            # Finally we pass the primary key as index, for standardization.
            df_heart.set_index("time", inplace=True)

        # 2. Format the return object as a dict:
        # We pair the df with the name of its intended table as key. 
        df_dict = {
            "HeartRateIntraday": df_heart 
        }

        return df_dict

    def parse_sleep_response(self, response, date):

        # First, define template types for the dataframes to be extracted.
        # We'll construct the response dataframes from these templates,
        # and use them for type validation and type conversion.
        intraday_types = {
            "date": "datetime64[ns]",
            "time": "datetime64[ns]",
            "duration_seconds": "int64",
            "sleep_stage": "int64" 
        }
        summary_types = {
            "date": "datetime64[ns]",
            "totalMinutesAsleep": "int64",
            "totalTimeInBed": "int64",
            "deepMinutes": "int64",
            "lightMinutes": "int64",
            "remMinutes": "int64",
            "wakeMinutes": "int64",
            "totalSleepRecords": "int64",
            "sleepBreakTimes": "str" 
        }

        # 1. Build the intraday dataframe: 
        # First, we extract the relevant data (i.e. subdicts) and parse it into
        # a first dataframe. This provides a first test of the response format. 
        df_response = None
        try: 
            # First, get the list of datasets; each represents a "chunk"
            # of sleep during the night, and we'll get multiple datasets 
            # for nights with broken sleep. 
            datasets_list = response["sleep"]

            # Each dataset contains the intraday data under the "levels" key. 
            # This data comes in 3 types:
            # 
            #   - Short cycles: Short "wake" periods (<= 180 sec). 
            #                   Listed under "shortData" key.
            # 
            #   - Long cycles:  Light, deep, rem, wake periods (> 180 sec).
            #                   Listed under "data" key.
            # 
            #   - "Default values":  
            #                   Awake, restless, asleep. 
            #                   Served when a sleep period (dataset) is too
            #                   short (~1 hour), or when sensor data quality 
            #                   is poor. 
            #                   Listed under "data" key.
            #
            # Each of these levels dataset is a list of dicts. We assemble
            # all of them together and parse them into a dataframe.

            levels_datasets = [] 
            for dataset in datasets_list: 
                levels = dataset["levels"]

                if "data" in levels:  # Long cycles and default values.
                    levels_datasets.extend(levels["data"]) 

                if "shortData" in levels:  # Short cycles.
                    levels_datasets.extend(levels["shortData"])  

            df_response = pd.DataFrame(levels_datasets)  

        except:  # Log bad response format.
            pass  # TODO (Future): add logging here. 

        # Next, we construct a plain dataframe from the template.
        # We then populate it using the above dataframe, adding a layer
        # of data validation and type conversion.  
        df_intraday = None
        if df_response is not None: 

            # Initialise null dataframe with appropriate shape and columns.
            num_rows = len(df_response.index) 
            df_intraday = pd.DataFrame(data=None, index=range(num_rows)) 

            for column in intraday_types.keys():
                df_intraday[column] = None

            # Fill in columns, handling type checks and conversions. 
            with contextlib.suppress(KeyError, TypeError):  # time
                df_intraday["time"] = df_response["dateTime"].astype(
                                                    intraday_types["time"]
                                                    )
                
            with contextlib.suppress(KeyError, TypeError):  # duration_seconds
                df_intraday["duration_seconds"] = df_response["seconds"].astype(
                                                    intraday_types["duration_seconds"]
                                                    )

            # We convert the sleep stage from str to an int, for memory reasons. 
            sleep_stage_id = {
                "deep": 1,          # normal expected data
                "light": 2,         #
                "rem": 3,           #
                "wake": 4,          #
                "asleep": 5,        # default values served
                "restless": 6,      #
                "awake": 7          # 
            }
            with contextlib.suppress(KeyError, TypeError):  # sleep_stage 
                df_intraday["sleep_stage"] = df_response["level"].map(
                                                            sleep_stage_id
                                                            )

            # Validation: only one sleep stage exists for each time.
            # Since we aggregated multiple sleep datasets into one
            # there may be overlaps coming from the short cycles, e.g.:
            #   - time: 11:45:00 --> stage: 2, duration 1800
            #   - time: 11:45:00 --> stage: 4, duration 30 
            # 
            # Here we choose a single sleep stage for each duplicate,
            # prioritising long cycles over others.  
            df_intraday.sort_values(by=["time", "duration_seconds"],
                                    ascending=[True, True],
                                    inplace=True)
            df_intraday.drop_duplicates(
                subset='time', keep="last", inplace=True)


            # Next, we timestamp the dataset to that day. 
            df_intraday["date"] = date 

            # Finally we pass the primary key as index, for standardization.
            df_intraday.set_index("time", inplace=True) 


        # 2. Build the summary dataframe: 
        # Similarly to 1, we extract the relevant data into a first dataframe.
        # Then we populate a new dataframe from our template, adding a layer
        # of data validation and type conversion. 
        df_response = None 
        try:
            summary = response["summary"]  

            # The summary dict has a single subdict containing the time
            # spent in each stage. We flatten this dict by bringing the stages
            # values up one level. 
            for stage_name in summary["stages"]:
                summary[stage_name] = summary["stages"][stage_name] 

            summary = {k: v for k, v in summary.items() if v != "stages"}

            # This dict is supposed to contain the raw data for all columns, 
            # except for:
            #   - "date" which we pass manually at the end, and for 
            #   - "sleepBreakTimes" which we'll have to assemble.  
            df_response = pd.DataFrame(summary, index=[0])  
            
        except:  # Log bad response format (parse failure).
            pass  # TODO (Future): add logging here.

        # Construct our dataframe, converting the response dataframe. 
        df_summary = None
        if df_response is not None: 

            # Initialise null dataframe with appropriate shape and columns.
            num_rows = len(df_response.index) 
            df_summary = pd.DataFrame(data=None, index=range(num_rows))

            for column in summary_types.keys():
                df_summary[column] = None

            # Fill in columns, handling type checks and conversions. 
            with contextlib.suppress(KeyError, TypeError):  # totalMinutesAsleep
                col = "totalMinutesAsleep"
                df_summary[col] = df_response[col].astype(summary_types[col])

            with contextlib.suppress(KeyError, TypeError):  # totalTimeInBed
                col = "totalTimeInBed"
                df_summary[col] = df_response[col].astype(summary_types[col])

            with contextlib.suppress(KeyError, TypeError):  # totalSleepRecords
                col = "totalSleepRecords"
                df_summary[col] = df_response[col].astype(summary_types[col])

            with contextlib.suppress(KeyError, TypeError):  # deepMinutes
                df_summary["deepMinutes"] = df_response["deep"].astype(
                                                    summary_types["deepMinutes"]
                                                    )

            with contextlib.suppress(KeyError, TypeError):  # remMinutes
                df_summary["remMinutes"] = df_response["rem"].astype(
                                                    summary_types["remMinutes"]
                                                    )

            with contextlib.suppress(KeyError, TypeError):  # lightMinutes
                df_summary["lightMinutes"] = df_response["light"].astype(
                                                    summary_types["lightMinutes"]
                                                    )

            with contextlib.suppress(KeyError, TypeError):  # wakeMinutes
                df_summary["wakeMinutes"] = df_response["wake"].astype(
                                                    summary_types["wakeMinutes"]
                                                    )

            # Now, we collect a list of sleep interruption times in the
            # sleepBreakTimes column. We'll extract it from the sleep
            # datasets which we looked at above. 
            # If there are no sleep interruption, a null value is recorded.
            sleepBreakTimes_list = []

            try:  # Read response data again. 
                datasets_list = response["sleep"] 

                if len(datasets_list) > 1:  # if sleep is broken

                    for dataset in datasets_list: 
                        try: 
                            end_time = dataset["endTime"] 
                            sleepBreakTimes_list.append(end_time) 
                        except:
                            pass

            except:
                pass

            # We have many sleep interruption times for a given night;
            # sort them from earliest to latest. 
            sleepBreakTimes_list.sort(key=lambda x: pd.to_datetime(x)) 
            
            # Remove last timestamp: this is when I wake up.
            if sleepBreakTimes_list:
                sleepBreakTimes_list.pop()

            # Assemble the result into a semicolon separated string. 
            sleepBreakTimes = None
            if sleepBreakTimes_list:
                sleepBreakTimes = ";".join(sleepBreakTimes_list) 
                df_summary["sleepBreakTimes"] = sleepBreakTimes 

            # Next, we datastamp the dataframe.
            df_summary["date"] = date  

            # Reorder columns.
            df_summary = df_summary[list(summary_types.keys())]

            # Finally we pass the primary key as index, for standardization.
            df_summary.set_index("date", inplace=True) 
             

        # 3. Format the return object as a dict: 
        # We pair each df with the name of its intended table as key.
        df_dict = {
            "SleepIntraday": df_intraday,
            "SleepDailySummary": df_summary
        }

        return df_dict

