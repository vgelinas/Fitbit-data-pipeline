"""
Unit tests for the methods which parse fitbit responses into dataframes.
"""
from pandas import Timestamp
from pipeline import ResponseParser
import numpy as np
import pandas as pd


def test_parse_activities_response():

    parser = ResponseParser()

    # ----------------- TEST 1 - Actual response ------------------------------
    date = pd.to_datetime('2020-05-01')
    response = {'activities': [{
               'activityId': 90013,
               'activityParentId': 90013,
               'activityParentName': 'Walk',
               'calories': 302,
               'description': 'Walking less than 2 mph, strolling very slowly',
               'duration': 2714000,
               'hasActiveZoneMinutes': False,
               'hasStartTime': True,
               'isFavorite': False,
               'lastModified': '2020-05-01T13:10:18.000Z',
               'logId': 30758911349,
               'name': 'Walk',
               'startDate': '2020-05-01',
               'startTime': '08:20',
               'steps': 4000}],
             'goals': {'activeMinutes': 30,
              'caloriesOut': 2745,
              'distance': 8.05,
              'steps': 12500},
             'summary': {'activeScore': -1,
              'activityCalories': 1379,
              'caloriesBMR': 1659,
              'caloriesOut': 2826,
              'distances': [{'activity': 'total', 'distance': 7.55},
               {'activity': 'tracker', 'distance': 7.55},
               {'activity': 'loggedActivities', 'distance': 0},
               {'activity': 'veryActive', 'distance': 4.18},
               {'activity': 'moderatelyActive', 'distance': 0.65},
               {'activity': 'lightlyActive', 'distance': 2.7},
               {'activity': 'sedentaryActive', 'distance': 0}],
              'fairlyActiveMinutes': 25,
              'heartRateZones': [{'caloriesOut': 1916.74877,
                'max': 94,
                'min': 30,
                'minutes': 1255,
                'name': 'Out of Range'},
               {'caloriesOut': 775.70893,
                'max': 132,
                'min': 94,
                'minutes': 137,
                'name': 'Fat Burn'},
               {'caloriesOut': 81.56868,
                'max': 160,
                'min': 132,
                'minutes': 8,
                'name': 'Cardio'},
               {'caloriesOut': 0, 'max': 220, 'min': 160, 'minutes': 0, 'name': 'Peak'}],
              'lightlyActiveMinutes': 210,
              'marginalCalories': 828,
              'restingHeartRate': 59,
              'sedentaryMinutes': 578,
              'steps': 9887,
              'veryActiveMinutes': 49}}

    # expected answer
    activities_answer_dict = {
        "logId": 30758911349,
        "activityId": 90013,
        "activityParentId": 90013,
        "activityParentName": "Walk",
        "name": "Walk",
        "description": "Walking less than 2 mph, strolling very slowly",
        "hasStartTime": True,
        "isFavorite": False,
        "hasActiveZoneMinutes": False,
        "date": pd.to_datetime("2020-05-01"),
        "startDateTime": pd.to_datetime("2020-05-01 08:20:00"),
        "endDateTime": pd.to_datetime("2020-05-01 09:05:00"),
        "durationMinutes": 45,
        "steps": 4000,
        "calories": 302
        }
    df_activities_answer = pd.DataFrame(activities_answer_dict, index=[0])
    df_activities_answer.set_index("logId", inplace=True)

    summary_answer_dict = {
        "date": pd.to_datetime("2020-05-01"),
        "activeScore": -1,
        "activityCalories": 1379,
        "caloriesBMR": 1659,
        "caloriesOut": 2826,
        "marginalCalories": 828,
        "sedentaryMinutes": 578,
        "lightlyActiveMinutes": 210,
        "fairlyActiveMinutes": 25,
        "veryActiveMinutes": 49,
        "restingHeartRate": 59,
        "steps": 9887,
        }
    df_summary_answer = pd.DataFrame(summary_answer_dict, index=[0])
    df_summary_answer.set_index("date", inplace=True)

    # apply parsing function
    # output is iterable of tuples, we'll collect key: value pairs in a dict
    df_dict = parser.parse_activities_response(response, date)

    df_activities = df_dict["Activities"]
    df_summary = df_dict["ActivitiesDailySummary"]

    # test activities dataframe
    assert(df_activities.shape == (1, 14))
    pd.testing.assert_frame_equal(df_activities, df_activities_answer)

    # test summary dataframe
    assert(df_summary.shape == (1, 11))
    pd.testing.assert_frame_equal(df_summary, df_summary_answer)


def test_parse_steps_response():

    parser = ResponseParser()

    # ------------------ TEST 1 - Actual response ----------------------------
    date = pd.to_datetime('2020-05-01')
    response = {
        'activities-steps': [{'dateTime': '2020-05-01', 'value': '9887'}],
        'activities-steps-intraday': {
                'dataset': [{'time': '00:00:00', 'value': 0},
                            {'time': '08:21:00', 'value': 102}],
                'datasetInterval': 1,
                'datasetType': 'minute'
                }
            }

    # expected answer
    steps_dict = {
        "date": [pd.to_datetime("2020-05-01"),
                 pd.to_datetime("2020-05-01")],
        "time": [pd.to_datetime("2020-05-01 00:00:00"),
                 pd.to_datetime("2020-05-01 08:21:00")],
        "num_steps": [0,
                      102]
    }
    df_steps_answer = pd.DataFrame(steps_dict, index=[0,1]).set_index("time")

    # apply parsing function
    df_dict = parser.parse_steps_response(response, date)
    df_steps = df_dict["ActivitiesStepsIntraday"]

    # test steps dataframe
    assert(df_steps.shape == (2, 2))
    pd.testing.assert_frame_equal(df_steps, df_steps_answer)

    # ------------------- TEST 2 - missing dataset subdict --------------------
    response = {
        'activities-steps': [{'dateTime': '2020-05-01', 'value': '9887'}],
        'activities-steps-intraday': {
                'dataset': [],
                'datasetInterval': 1,
                'datasetType': 'minute'
                }
            }

    # apply parsing function
    df_dict = parser.parse_steps_response(response, date)
    df_steps = df_dict["ActivitiesStepsIntraday"]

    # test steps dataframe
    assert(df_steps is None)


def test_parse_heart_rate_response():

    parser = ResponseParser()

    # ------------------- TEST 1 - Actual response ----------------------------
    date = pd.to_datetime('2020-05-01')
    response = {'activities-heart':
    [{'dateTime': '2020-05-01',
   'value': {'customHeartRateZones': [],
    'heartRateZones': [{'caloriesOut': 1916.74877,
      'max': 94,
      'min': 30,
      'minutes': 1255,
      'name': 'Out of Range'},
     {'caloriesOut': 775.70893,
      'max': 132,
      'min': 94,
      'minutes': 137,
      'name': 'Fat Burn'},
     {'caloriesOut': 81.56868,
      'max': 160,
      'min': 132,
      'minutes': 8,
      'name': 'Cardio'},
     {'caloriesOut': 0, 'max': 220, 'min': 160, 'minutes': 0, 'name': 'Peak'}],
    'restingHeartRate': 59}}],
    'activities-heart-intraday':{
                    'dataset': [{'time': '00:00:00', 'value': 69},
                            {'time': '17:17:00', 'value': 140}],
                    'datasetInterval': 1,
                    'datasetType': 'minute'}
    }

    # expected answer
    heart_dict = {
        "date": [pd.to_datetime("2020-05-01"),
                 pd.to_datetime("2020-05-01")],
        "time": [pd.to_datetime("2020-05-01 00:00:00"),
                 pd.to_datetime("2020-05-01 17:17:00")],
        "bpm":  [69,
                 140]
    }
    df_heart_answer = pd.DataFrame(heart_dict, index=[0,1]).set_index("time")

    # apply parsing function
    df_dict = parser.parse_heart_rate_response(response, date)
    df_heart = df_dict["HeartRateIntraday"]

    # test heart rate dataframe
    assert(df_heart.shape == (2, 2))
    pd.testing.assert_frame_equal(df_heart, df_heart_answer)

    # ----------------- TEST 2 - missing dataset subdict ----------------------
    response = {'activities-heart':
    [{'dateTime': '2020-05-01',
   'value': {'customHeartRateZones': [],
    'heartRateZones': [{'caloriesOut': 1916.74877,
      'max': 94,
      'min': 30,
      'minutes': 1255,
      'name': 'Out of Range'},
     {'caloriesOut': 775.70893,
      'max': 132,
      'min': 94,
      'minutes': 137,
      'name': 'Fat Burn'},
     {'caloriesOut': 81.56868,
      'max': 160,
      'min': 132,
      'minutes': 8,
      'name': 'Cardio'},
     {'caloriesOut': 0, 'max': 220, 'min': 160, 'minutes': 0, 'name': 'Peak'}],
    'restingHeartRate': 59}}],
   'activities-heart-intraday':{
                    'dataset': [],
                    'datasetInterval': 1,
                    'datasetType': 'minute'}
    }

    # apply parsing function
    df_dict = parser.parse_heart_rate_response(response, date)
    df_heart = df_dict["HeartRateIntraday"]

    # test heart rate dataframe
    assert(df_heart is None)


def test_parse_sleep_response():

    parser = ResponseParser()

    # ------------------ TEST 1 - Actual response -----------------------------
    # We test the response on 2021-07-24 because it has a broken sleep dataset
    # To test missing value handling, some sleep stage values were set to nan
    # response dict is shorted slightly for convenience since we store it here
    date = pd.to_datetime('2021-07-24')
    response = {
        'sleep': [
            {'dateOfSleep': '2021-07-24',
             'duration': 17820000,
             'efficiency': 94,
             'endTime': '2021-07-24T10:26:30.000',
             'infoCode': 0,
             'isMainSleep': True,
             'levels': {
                'data': [
                    {'dateTime': '2021-07-24T05:29:00.000',
                     'level': 'wake', 'seconds': 510},
                    {'dateTime': '2021-07-24T05:37:30.000',
                     'level': 'light', 'seconds': 120},
                    {'dateTime': '2021-07-24T05:39:30.000',
                     'level': 'wake', 'seconds': 480},
                    {'dateTime': '2021-07-24T05:47:30.000',
                     'level': 'light', 'seconds': 3900},
                    {'dateTime': '2021-07-24T06:52:30.000',
                     'level': 'deep', 'seconds': 1440},
                    {'dateTime': '2021-07-24T07:16:30.000',
                     'level': 'light', 'seconds': 720},
                    {'dateTime': '2021-07-24T07:28:30.000',
                     'level': 'deep', 'seconds': 480},
                    {'dateTime': '2021-07-24T07:36:30.000',
                     'level': 'light', 'seconds': 2070},
                    {'dateTime': '2021-07-24T08:11:00.000',
                     'level': 'rem', 'seconds': 990},
                    {'dateTime': '2021-07-24T08:27:30.000',
                     'level': 'light', 'seconds': 1500},
                    {'dateTime': '2021-07-24T08:52:30.000',
                     'level': 'deep', 'seconds': 1260},
                    {'dateTime': '2021-07-24T09:13:30.000',
                     'level': 'light', 'seconds': 660},
                    {'dateTime': '2021-07-24T09:24:30.000',
                     'level': 'rem', 'seconds': 900},
                    {'dateTime': '2021-07-24T09:39:30.000',
                     'level': 'light', 'seconds': 2070},
                    {'dateTime': '2021-07-24T10:14:00.000',
                     'level': 'wake', 'seconds': 750}
                     ],
                'shortData': [
                    {'dateTime': '2021-07-24T05:49:30.000',
                     'level': 'wake', 'seconds': 30},
                    {'dateTime': '2021-07-24T06:02:00.000',
                     'level': 'wake', 'seconds': 30},
                    {'dateTime': '2021-07-24T06:14:00.000',
                     'level': 'wake', 'seconds': 60},
                    {'dateTime': '2021-07-24T06:24:00.000',
                     'level': 'wake', 'seconds': 30},
                    {'dateTime': '2021-07-24T06:27:30.000',
                     'level': 'wake', 'seconds': 60},
                    {'dateTime': '2021-07-24T07:38:30.000',
                     'level': 'wake', 'seconds': 30},
                    {'dateTime': '2021-07-24T07:41:00.000',
                     'level': 'wake', 'seconds': 30},
                    {'dateTime': '2021-07-24T07:48:30.000',
                     'level': 'wake', 'seconds': 30},
                    {'dateTime': '2021-07-24T07:56:30.000',
                     'level': 'wake', 'seconds': 60},
                    {'dateTime': '2021-07-24T08:23:00.000',
                     'level': 'wake', 'seconds': 30},
                    {'dateTime': '2021-07-24T08:26:30.000',
                     'level': 'wake', 'seconds': 60},
                    {'dateTime': '2021-07-24T08:32:00.000',
                     'level': 'wake', 'seconds': 60},
                    {'dateTime': '2021-07-24T08:42:30.000',
                     'level': 'wake', 'seconds': 30},
                    {'dateTime': '2021-07-24T09:18:30.000',
                     'level': 'wake', 'seconds': 60},
                    {'dateTime': '2021-07-24T09:28:30.000',
                     'level': 'wake', 'seconds': 30},
                    {'dateTime': '2021-07-24T10:01:30.000',
                     'level': 'wake', 'seconds': 30}
                     ],
                'summary': {
                    'deep': {
                        'count': 3,
                        'minutes': 53,
                        'thirtyDayAvgMinutes': 87
                        },
                    'light': {
                        'count': 20,
                        'minutes': 175,
                        'thirtyDayAvgMinutes': 292
                        },
                    'rem': {
                        'count': 4,
                        'minutes': 29,
                        'thirtyDayAvgMinutes': 95
                        },
                    'wake': {
                        'count': 19,
                        'minutes': 40,
                        'thirtyDayAvgMinutes': 76
                        }
                    }
                },
                'logId': 33100848824,
                'minutesAfterWakeup': 0,
                'minutesAsleep': 257,
                'minutesAwake': 40,
                'minutesToFallAsleep': 0,
                'startTime': '2021-07-24T05:29:00.000',
                'timeInBed': 297,
                'type': 'stages'
             },
            {'dateOfSleep': '2021-07-24',
             'duration': 13800000,
             'efficiency': 91,
             'endTime': '2021-07-24T04:22:30.000',
             'infoCode': 0,
             'isMainSleep': False,
             'levels': {
                'data': [
                    {'dateTime': '2021-07-24T00:32:00.000',
                     'level': 'wake', 'seconds': 30},
                    {'dateTime': '2021-07-24T00:32:30.000',
                     'level': 'light', 'seconds': 720},
                    {'dateTime': '2021-07-24T00:44:30.000',
                     'level': 'rem', 'seconds': 1050},
                    {'dateTime': '2021-07-24T01:02:00.000',
                     'level': 'light', 'seconds': 840},
                    {'dateTime': '2021-07-24T01:16:00.000',
                     'level': 'deep', 'seconds': 480},
                    {'dateTime': '2021-07-24T01:24:00.000',
                     'level': 'light', 'seconds': 1170},
                    {'dateTime': '2021-07-24T01:43:30.000',
                     'level': 'wake', 'seconds': 300},
                    {'dateTime': '2021-07-24T01:48:30.000',
                     'level': 'light', 'seconds': 120},
                    {'dateTime': '2021-07-24T01:50:30.000',
                     'level': 'rem', 'seconds': 780},
                    {'dateTime': '2021-07-24T02:03:30.000',
                     'level': 'light', 'seconds': 1860},
                    {'dateTime': '2021-07-24T02:34:30.000',
                     'level': 'deep', 'seconds': 1800},
                    {'dateTime': '2021-07-24T03:04:30.000',
                     'level': 'light', 'seconds': 1740},
                    {'dateTime': '2021-07-24T03:33:30.000',
                     'level': 'wake', 'seconds': 660},
                    {'dateTime': '2021-07-24T03:44:30.000',
                     'level': 'light', 'seconds': 300},
                    {'dateTime': '2021-07-24T03:49:30.000',
                     'level': 'wake', 'seconds': 210},
                    {'dateTime': '2021-07-24T03:53:00.000',
                     'level': 'light', 'seconds': 270},
                    {'dateTime': '2021-07-24T03:57:30.000',
                     'level': 'wake', 'seconds': 540},
                    {'dateTime': '2021-07-24T04:06:30.000',
                     'level': 'light', 'seconds': 270},
                    {'dateTime': '2021-07-24T04:11:00.000',
                     'level': 'wake', 'seconds': 360},
                    {'dateTime': '2021-07-24T04:17:00.000',
                     'level': 'light', 'seconds': 330}
                     ],
                'shortData': [
                    {'dateTime': '2021-07-24T00:32:00.000',
                     'level': 'wake', 'seconds': 30},
                    {'dateTime': '2021-07-24T01:24:00.000',
                     'level': 'wake', 'seconds': 30},
                    {'dateTime': '2021-07-24T02:03:30.000',
                     'level': 'wake', 'seconds': 30},
                    {'dateTime': '2021-07-24T02:19:00.000',
                     'level': np.nan, 'seconds': 30},
                    {'dateTime': '2021-07-24T03:02:30.000',
                     'level': 'wake', 'seconds': 120},
                    {'dateTime': '2021-07-24T03:12:00.000',
                     'level': 'wake', 'seconds': 60},
                    {'dateTime': '2021-07-24T03:29:30.000',
                     'level': 'wake', 'seconds': 60},
                    {'dateTime': '2021-07-24T03:54:30.000',
                     'level': 'wake', 'seconds': 30},
                    {'dateTime': '2021-07-24T04:21:00.000',
                     'level': 'wake', 'seconds': 90}],
                'summary': {
                    'deep': {
                        'count': 2,
                        'minutes': 36,
                        'thirtyDayAvgMinutes': 87
                        },
                    'light': {
                        'count': 14,
                        'minutes': 122,
                        'thirtyDayAvgMinutes': 292
                        },
                    'rem': {
                        'count': 2,
                        'minutes': 30,
                        'thirtyDayAvgMinutes': 95
                        },
                    'wake': {
                        'count': 14,
                        'minutes': 42,
                        'thirtyDayAvgMinutes': 76
                        }
                    }
                },
                'logId': 33097957310,
                'minutesAfterWakeup': 0,
                'minutesAsleep': 188,
                'minutesAwake': 42,
                'minutesToFallAsleep': 0,
                'startTime': '2021-07-24T00:32:00.000',
                'timeInBed': 230,
                'type': 'stages'}
                ],
        'summary': {
            'stages': {
                'deep': 89,
                'light': 297,
                'rem': 60,
                'wake': 82
                },
            'totalMinutesAsleep': 445,
            'totalSleepRecords': 2,
            'totalTimeInBed': 527
            }
        }

    # expected answer
    summary_answer_dict = {
        "date": pd.to_datetime("2021-07-24"),
        "totalMinutesAsleep": 445,
        "totalTimeInBed": 527,
        "deepMinutes": 89,
        "lightMinutes": 297,
        "remMinutes": 60,
        "wakeMinutes": 82,
        "totalSleepRecords": 2,
        "sleepBreakTimes": "2021-07-24T04:22:30.000"
    }
    df_summary_answer = pd.DataFrame(summary_answer_dict, index=[0])
    df_summary_answer.set_index("date", inplace=True)

    intraday_answer_dict = {
        'sleep_stage': {
            Timestamp('2021-07-24 00:32:00'): 4,
            Timestamp('2021-07-24 00:32:30'): 2,
            Timestamp('2021-07-24 00:44:30'): 3,
            Timestamp('2021-07-24 01:02:00'): 2,
            Timestamp('2021-07-24 01:16:00'): 1,
            Timestamp('2021-07-24 01:24:00'): 2,
            Timestamp('2021-07-24 01:43:30'): 4,
            Timestamp('2021-07-24 01:48:30'): 2,
            Timestamp('2021-07-24 01:50:30'): 3,
            Timestamp('2021-07-24 02:03:30'): 2,
            Timestamp('2021-07-24 02:19:00'): None, # testing missing value
            Timestamp('2021-07-24 02:34:30'): 1,
            Timestamp('2021-07-24 03:02:30'): 4,
            Timestamp('2021-07-24 03:04:30'): 2,
            Timestamp('2021-07-24 03:12:00'): 4,
            Timestamp('2021-07-24 03:29:30'): 4,
            Timestamp('2021-07-24 03:33:30'): 4,
            Timestamp('2021-07-24 03:44:30'): 2,
            Timestamp('2021-07-24 03:49:30'): 4,
            Timestamp('2021-07-24 03:53:00'): 2,
            Timestamp('2021-07-24 03:54:30'): 4,
            Timestamp('2021-07-24 03:57:30'): 4,
            Timestamp('2021-07-24 04:06:30'): 2,
            Timestamp('2021-07-24 04:11:00'): 4,
            Timestamp('2021-07-24 04:17:00'): 2,
            Timestamp('2021-07-24 04:21:00'): 4,
            Timestamp('2021-07-24 05:29:00'): 4,
            Timestamp('2021-07-24 05:37:30'): 2,
            Timestamp('2021-07-24 05:39:30'): 4,
            Timestamp('2021-07-24 05:47:30'): 2,
            Timestamp('2021-07-24 05:49:30'): 4,
            Timestamp('2021-07-24 06:02:00'): 4,
            Timestamp('2021-07-24 06:14:00'): 4,
            Timestamp('2021-07-24 06:24:00'): 4,
            Timestamp('2021-07-24 06:27:30'): 4,
            Timestamp('2021-07-24 06:52:30'): 1,
            Timestamp('2021-07-24 07:16:30'): 2,
            Timestamp('2021-07-24 07:28:30'): 1,
            Timestamp('2021-07-24 07:36:30'): 2,
            Timestamp('2021-07-24 07:38:30'): 4,
            Timestamp('2021-07-24 07:41:00'): 4,
            Timestamp('2021-07-24 07:48:30'): 4,
            Timestamp('2021-07-24 07:56:30'): 4,
            Timestamp('2021-07-24 08:11:00'): 3,
            Timestamp('2021-07-24 08:23:00'): 4,
            Timestamp('2021-07-24 08:26:30'): 4,
            Timestamp('2021-07-24 08:27:30'): 2,
            Timestamp('2021-07-24 08:32:00'): 4,
            Timestamp('2021-07-24 08:42:30'): 4,
            Timestamp('2021-07-24 08:52:30'): 1,
            Timestamp('2021-07-24 09:13:30'): 2,
            Timestamp('2021-07-24 09:18:30'): 4,
            Timestamp('2021-07-24 09:24:30'): 3,
            Timestamp('2021-07-24 09:28:30'): 4,
            Timestamp('2021-07-24 09:39:30'): 2,
            Timestamp('2021-07-24 10:01:30'): 4,
            Timestamp('2021-07-24 10:14:00'): 4
            },
        'duration_seconds': {
            Timestamp('2021-07-24 00:32:00'): 30,
            Timestamp('2021-07-24 00:32:30'): 720,
            Timestamp('2021-07-24 00:44:30'): 1050,
            Timestamp('2021-07-24 01:02:00'): 840,
            Timestamp('2021-07-24 01:16:00'): 480,
            Timestamp('2021-07-24 01:24:00'): 1170,
            Timestamp('2021-07-24 01:43:30'): 300,
            Timestamp('2021-07-24 01:48:30'): 120,
            Timestamp('2021-07-24 01:50:30'): 780,
            Timestamp('2021-07-24 02:03:30'): 1860,
            Timestamp('2021-07-24 02:19:00'): 30,
            Timestamp('2021-07-24 02:34:30'): 1800,
            Timestamp('2021-07-24 03:02:30'): 120,
            Timestamp('2021-07-24 03:04:30'): 1740,
            Timestamp('2021-07-24 03:12:00'): 60,
            Timestamp('2021-07-24 03:29:30'): 60,
            Timestamp('2021-07-24 03:33:30'): 660,
            Timestamp('2021-07-24 03:44:30'): 300,
            Timestamp('2021-07-24 03:49:30'): 210,
            Timestamp('2021-07-24 03:53:00'): 270,
            Timestamp('2021-07-24 03:54:30'): 30,
            Timestamp('2021-07-24 03:57:30'): 540,
            Timestamp('2021-07-24 04:06:30'): 270,
            Timestamp('2021-07-24 04:11:00'): 360,
            Timestamp('2021-07-24 04:17:00'): 330,
            Timestamp('2021-07-24 04:21:00'): 90,
            Timestamp('2021-07-24 05:29:00'): 510,
            Timestamp('2021-07-24 05:37:30'): 120,
            Timestamp('2021-07-24 05:39:30'): 480,
            Timestamp('2021-07-24 05:47:30'): 3900,
            Timestamp('2021-07-24 05:49:30'): 30,
            Timestamp('2021-07-24 06:02:00'): 30,
            Timestamp('2021-07-24 06:14:00'): 60,
            Timestamp('2021-07-24 06:24:00'): 30,
            Timestamp('2021-07-24 06:27:30'): 60,
            Timestamp('2021-07-24 06:52:30'): 1440,
            Timestamp('2021-07-24 07:16:30'): 720,
            Timestamp('2021-07-24 07:28:30'): 480,
            Timestamp('2021-07-24 07:36:30'): 2070,
            Timestamp('2021-07-24 07:38:30'): 30,
            Timestamp('2021-07-24 07:41:00'): 30,
            Timestamp('2021-07-24 07:48:30'): 30,
            Timestamp('2021-07-24 07:56:30'): 60,
            Timestamp('2021-07-24 08:11:00'): 990,
            Timestamp('2021-07-24 08:23:00'): 30,
            Timestamp('2021-07-24 08:26:30'): 60,
            Timestamp('2021-07-24 08:27:30'): 1500,
            Timestamp('2021-07-24 08:32:00'): 60,
            Timestamp('2021-07-24 08:42:30'): 30,
            Timestamp('2021-07-24 08:52:30'): 1260,
            Timestamp('2021-07-24 09:13:30'): 660,
            Timestamp('2021-07-24 09:18:30'): 60,
            Timestamp('2021-07-24 09:24:30'): 900,
            Timestamp('2021-07-24 09:28:30'): 30,
            Timestamp('2021-07-24 09:39:30'): 2070,
            Timestamp('2021-07-24 10:01:30'): 30,
            Timestamp('2021-07-24 10:14:00'): 750},
        'date': {
            Timestamp('2021-07-24 00:32:00'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 00:32:30'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 00:44:30'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 01:02:00'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 01:16:00'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 01:24:00'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 01:43:30'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 01:48:30'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 01:50:30'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 02:03:30'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 02:19:00'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 02:34:30'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 03:02:30'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 03:04:30'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 03:12:00'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 03:29:30'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 03:33:30'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 03:44:30'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 03:49:30'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 03:53:00'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 03:54:30'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 03:57:30'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 04:06:30'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 04:11:00'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 04:17:00'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 04:21:00'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 05:29:00'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 05:37:30'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 05:39:30'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 05:47:30'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 05:49:30'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 06:02:00'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 06:14:00'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 06:24:00'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 06:27:30'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 06:52:30'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 07:16:30'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 07:28:30'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 07:36:30'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 07:38:30'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 07:41:00'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 07:48:30'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 07:56:30'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 08:11:00'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 08:23:00'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 08:26:30'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 08:27:30'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 08:32:00'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 08:42:30'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 08:52:30'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 09:13:30'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 09:18:30'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 09:24:30'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 09:28:30'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 09:39:30'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 10:01:30'): Timestamp('2021-07-24 00:00:00'),
            Timestamp('2021-07-24 10:14:00'): Timestamp('2021-07-24 00:00:00')
            },
        'time': {
            Timestamp('2021-07-24 00:32:00'): Timestamp('2021-07-24 00:32:00'),
            Timestamp('2021-07-24 00:32:30'): Timestamp('2021-07-24 00:32:30'),
            Timestamp('2021-07-24 00:44:30'): Timestamp('2021-07-24 00:44:30'),
            Timestamp('2021-07-24 01:02:00'): Timestamp('2021-07-24 01:02:00'),
            Timestamp('2021-07-24 01:16:00'): Timestamp('2021-07-24 01:16:00'),
            Timestamp('2021-07-24 01:24:00'): Timestamp('2021-07-24 01:24:00'),
            Timestamp('2021-07-24 01:43:30'): Timestamp('2021-07-24 01:43:30'),
            Timestamp('2021-07-24 01:48:30'): Timestamp('2021-07-24 01:48:30'),
            Timestamp('2021-07-24 01:50:30'): Timestamp('2021-07-24 01:50:30'),
            Timestamp('2021-07-24 02:03:30'): Timestamp('2021-07-24 02:03:30'),
            Timestamp('2021-07-24 02:19:00'): Timestamp('2021-07-24 02:19:00'),
            Timestamp('2021-07-24 02:34:30'): Timestamp('2021-07-24 02:34:30'),
            Timestamp('2021-07-24 03:02:30'): Timestamp('2021-07-24 03:02:30'),
            Timestamp('2021-07-24 03:04:30'): Timestamp('2021-07-24 03:04:30'),
            Timestamp('2021-07-24 03:12:00'): Timestamp('2021-07-24 03:12:00'),
            Timestamp('2021-07-24 03:29:30'): Timestamp('2021-07-24 03:29:30'),
            Timestamp('2021-07-24 03:33:30'): Timestamp('2021-07-24 03:33:30'),
            Timestamp('2021-07-24 03:44:30'): Timestamp('2021-07-24 03:44:30'),
            Timestamp('2021-07-24 03:49:30'): Timestamp('2021-07-24 03:49:30'),
            Timestamp('2021-07-24 03:53:00'): Timestamp('2021-07-24 03:53:00'),
            Timestamp('2021-07-24 03:54:30'): Timestamp('2021-07-24 03:54:30'),
            Timestamp('2021-07-24 03:57:30'): Timestamp('2021-07-24 03:57:30'),
            Timestamp('2021-07-24 04:06:30'): Timestamp('2021-07-24 04:06:30'),
            Timestamp('2021-07-24 04:11:00'): Timestamp('2021-07-24 04:11:00'),
            Timestamp('2021-07-24 04:17:00'): Timestamp('2021-07-24 04:17:00'),
            Timestamp('2021-07-24 04:21:00'): Timestamp('2021-07-24 04:21:00'),
            Timestamp('2021-07-24 05:29:00'): Timestamp('2021-07-24 05:29:00'),
            Timestamp('2021-07-24 05:37:30'): Timestamp('2021-07-24 05:37:30'),
            Timestamp('2021-07-24 05:39:30'): Timestamp('2021-07-24 05:39:30'),
            Timestamp('2021-07-24 05:47:30'): Timestamp('2021-07-24 05:47:30'),
            Timestamp('2021-07-24 05:49:30'): Timestamp('2021-07-24 05:49:30'),
            Timestamp('2021-07-24 06:02:00'): Timestamp('2021-07-24 06:02:00'),
            Timestamp('2021-07-24 06:14:00'): Timestamp('2021-07-24 06:14:00'),
            Timestamp('2021-07-24 06:24:00'): Timestamp('2021-07-24 06:24:00'),
            Timestamp('2021-07-24 06:27:30'): Timestamp('2021-07-24 06:27:30'),
            Timestamp('2021-07-24 06:52:30'): Timestamp('2021-07-24 06:52:30'),
            Timestamp('2021-07-24 07:16:30'): Timestamp('2021-07-24 07:16:30'),
            Timestamp('2021-07-24 07:28:30'): Timestamp('2021-07-24 07:28:30'),
            Timestamp('2021-07-24 07:36:30'): Timestamp('2021-07-24 07:36:30'),
            Timestamp('2021-07-24 07:38:30'): Timestamp('2021-07-24 07:38:30'),
            Timestamp('2021-07-24 07:41:00'): Timestamp('2021-07-24 07:41:00'),
            Timestamp('2021-07-24 07:48:30'): Timestamp('2021-07-24 07:48:30'),
            Timestamp('2021-07-24 07:56:30'): Timestamp('2021-07-24 07:56:30'),
            Timestamp('2021-07-24 08:11:00'): Timestamp('2021-07-24 08:11:00'),
            Timestamp('2021-07-24 08:23:00'): Timestamp('2021-07-24 08:23:00'),
            Timestamp('2021-07-24 08:26:30'): Timestamp('2021-07-24 08:26:30'),
            Timestamp('2021-07-24 08:27:30'): Timestamp('2021-07-24 08:27:30'),
            Timestamp('2021-07-24 08:32:00'): Timestamp('2021-07-24 08:32:00'),
            Timestamp('2021-07-24 08:42:30'): Timestamp('2021-07-24 08:42:30'),
            Timestamp('2021-07-24 08:52:30'): Timestamp('2021-07-24 08:52:30'),
            Timestamp('2021-07-24 09:13:30'): Timestamp('2021-07-24 09:13:30'),
            Timestamp('2021-07-24 09:18:30'): Timestamp('2021-07-24 09:18:30'),
            Timestamp('2021-07-24 09:24:30'): Timestamp('2021-07-24 09:24:30'),
            Timestamp('2021-07-24 09:28:30'): Timestamp('2021-07-24 09:28:30'),
            Timestamp('2021-07-24 09:39:30'): Timestamp('2021-07-24 09:39:30'),
            Timestamp('2021-07-24 10:01:30'): Timestamp('2021-07-24 10:01:30'),
            Timestamp('2021-07-24 10:14:00'): Timestamp('2021-07-24 10:14:00')
            }
        }
    df_intraday_answer = pd.DataFrame(intraday_answer_dict)
    df_intraday_answer.set_index("time", inplace=True)

    # reorder columns
    df_intraday_answer = df_intraday_answer[["date",
                                             "duration_seconds",
                                             "sleep_stage"]]

    # apply parsing function
    df_dict = parser.parse_sleep_response(response, date)

    df_summary = df_dict["SleepDailySummary"]
    df_intraday = df_dict["SleepIntraday"]

    # test summary dataframe
    assert(df_summary.shape == (1, 8))
    pd.testing.assert_frame_equal(df_summary, df_summary_answer)

    # test intraday dataframe
    assert(df_intraday.shape == (57, 3))
    pd.testing.assert_frame_equal(df_intraday, df_intraday_answer)
