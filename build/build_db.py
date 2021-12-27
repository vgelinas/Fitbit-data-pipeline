"""
Scripts to create db schema and populate key tables. Passing along an 
optional -dl flag launches full data download from the Fitbit web api.

Quickstart: python3 build_db.py -dl -v
"""

import argparse
import pandas as pd
import json
from sqlalchemy import inspect
from sqlalchemy.orm import sessionmaker
import db_connection
from db_tables import Base, FitbitCredentials, FitbitUserInfo, SleepStageId
from fitbit_api import Fitbit
from pipeline import Pipeline


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    # -v flag: make this script verbose.
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="increase output verbosity")
    # -dl flag: launch full data download from api following database setup.
    parser.add_argument("-dl", "--download_all", action="store_true",
                        help="launch complete data download from api")
    # -s flag: pipeline arg (number of seconds between api calls).
    parser.add_argument("-s", "--seconds_between_calls", type=int,
                        help="number of seconds between fitbit api calls")
    args = parser.parse_args()


    # Next, create all tables which don't currently exist. 
    engine = db_connection.create_engine()
    Session = sessionmaker(bind = engine)
    session = Session()

    if args.verbose:
        # List existing tables in db.
        inspector = inspect(engine)
        existing_tables = inspector.get_table_names()
        all_tables = [table.name for table in Base.metadata.sorted_tables]
        missing_tables = [t for t in all_tables if t not in existing_tables]

        if missing_tables:
            print("="*60)
            print("Creating the following tables:")
            print(*[">> " + table for table in missing_tables], sep="\n")
            print("="*60)
        else:
            print("All tables already exist.")

    # Table creation.
    Base.metadata.create_all(engine, checkfirst=True)


    # If FitbitCredentials table is empty, populate from flat file.
    if not session.query(FitbitCredentials).first():

        # Load credentials and starter tokens from flat file in build directory.
        if args.verbose:
            print("Populating FitbitCredentials table with flat tokens.")

        token_path = "/absolute/path/to/project/folder/build/"

        with open(token_path+"fitbit_starter_tokens.json") as f:
            flat_tokens_dict = json.load(f)

        user_creds = FitbitCredentials(id = 1,
                            client_id = flat_tokens_dict["client_id"],
                            client_secret = flat_tokens_dict["client_secret"],
                            access_token = flat_tokens_dict["access_token"],
                            refresh_token = flat_tokens_dict["refresh_token"]
                            )

        session.add(user_creds)
        session.commit()

        # Refresh tokens once for good measure, and then erase the flat tokens
        # if successful as they are no longer valid.
        Fitbit(session).refresh_tokens()

        flat_tokens_dict["access_token"] = ""
        flat_tokens_dict["refresh_token"] = ""
        with open(token_path+"fitbit_starter_tokens.json", 'w') as f:
            json.dump(flat_tokens_dict, f)


    # If FitbitUserInfo table is empty, populate from web api.
    if not session.query(FitbitUserInfo).first():
        if args.verbose:
            print("Populating FitbitUserInfo table by calling api.")

        fitbit = Fitbit(session)

        url = "https://api.fitbit.com/1/user/-/profile.json"
        response = fitbit.get_resource(url)
        if response.status_code == 200:
            response = response.json()
            start_date = response["user"]["memberSince"]
            start_date = pd.to_datetime(start_date)
            stride_length_running = response["user"]["strideLengthRunning"]
            stride_length_walking = response["user"]["strideLengthWalking"]

        user_start_date = FitbitUserInfo(
                             id=1,
                             start_date=start_date,
                             stride_length_running=stride_length_running,
                             stride_length_walking=stride_length_walking
                             )

        session.add(user_start_date)
        session.commit()

    # If SleepStageId is empty, populate manually.
    if not session.query(SleepStageId).first():

        if args.verbose:
            print("Populating SleepStageId table with default values.")

        # The standard 4 sleep stages, for most nights of sleep.
        deep = SleepStageId(id = 1, stage = 'deep')
        light = SleepStageId(id = 2, stage = 'light')
        rem = SleepStageId(id = 3, stage = 'rem')
        wake = SleepStageId(id = 4, stage = 'wake')

        # When the sleep period is too short (~1 hour) the normal stages
        # are not detected by Fitbit, which gives simpler "default" values.
        asleep = SleepStageId(id = 5, stage = 'asleep')
        restless = SleepStageId(id = 6, stage = 'restless')
        awake = SleepStageId(id = 7, stage = 'awake')

        session.add_all([deep, light, rem, deep, wake, awake, restless, asleep])
        session.commit()


    # (Optional: -dl flag): Download full data from web api.
    if args.download_all:

        if args.verbose:
            print("Downloading dataset. This might take up to ~24h.")

        # Collect pipeline command line args for initialisation.
        pipeline_args = {
            "seconds_between_calls": args.seconds_between_calls,
            "verbose": args.verbose
            }

        # When the user doesn't pass an argument, the dict value above is null.
        # Remove these key:value pairs as arguments, since we'll use the 
        # Pipeline's default init values for these parameters instead.
        pipeline_args = {k: v for k,v in pipeline_args.items() if v is not None}

        # Launch download!
        Pipeline(**pipeline_args).run()
