"""
Database table information for the sqlalchemy ORM.
"""
from sqlalchemy import Column
from sqlalchemy import Integer, BigInteger, Float, String, Boolean, DateTime
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()


# --------------------------- FitbitML TABLES ---------------------------------
class FitbitCredentials(Base):
    __tablename__ = 'fitbit_credentials'

    id = Column(Integer, primary_key=True)
    client_id = Column(String(255))
    client_secret = Column(String(255))
    access_token = Column(String(500))
    refresh_token = Column(String(500))
    expires_in = Column(String(255))
    expires_at = Column(String(255))
    scope = Column(String(255))
    token_type = Column(String(255))
    user_id = Column(String(255))


class FitbitUserInfo(Base):
    __tablename__ = 'fitbit_user_info'

    id = Column(Integer, primary_key=True)
    start_date = Column(DateTime)
    stride_length_running = Column(Float)
    stride_length_walking = Column(Float)


class Activities(Base):
    __tablename__ = 'activities'

    logId = Column(BigInteger, primary_key=True)
    activityId = Column(Integer)
    activityParentId = Column(Integer)
    activityParentName = Column(String(50))
    name = Column(String(50))
    description = Column(String(100))
    hasStartTime = Column(Boolean)
    isFavorite = Column(Boolean)
    hasActiveZoneMinutes = Column(Boolean)
    date = Column(DateTime)
    startDateTime = Column(DateTime)
    endDateTime = Column(DateTime)
    durationMinutes = Column(Integer)
    steps = Column(Integer)
    calories = Column(Integer)


class ActivitiesDailySummary(Base):
    __tablename__ = 'activities_daily_summary'

    date = Column(DateTime, primary_key=True)
    activeScore = Column(Integer)
    activityCalories = Column(Integer)
    caloriesBMR = Column(Integer)
    caloriesOut = Column(Integer)
    marginalCalories = Column(Integer)
    sedentaryMinutes = Column(Integer)
    lightlyActiveMinutes = Column(Integer)
    fairlyActiveMinutes = Column(Integer)
    veryActiveMinutes = Column(Integer)
    restingHeartRate = Column(Integer)
    steps = Column(Integer)


class ActivitiesStepsIntraday(Base):
    __tablename__ = 'activities_steps_intraday'

    date = Column(DateTime)
    time = Column(DateTime, primary_key=True)
    num_steps = Column(Integer)


class HeartRateIntraday(Base):
    __tablename__ = 'heart_rate_intraday'

    date = Column(DateTime)
    time = Column(DateTime, primary_key=True)
    bpm = Column(Integer)


class SleepIntraday(Base):
    __tablename__ = 'sleep_intraday'

    date = Column(DateTime)
    time = Column(DateTime, primary_key=True)
    duration_seconds = Column(Integer)
    sleep_stage = Column(Integer)


class SleepStageId(Base):
    __tablename__ = 'sleep_stage_id'

    id = Column(Integer, primary_key=True)
    stage = Column(String(8))


class SleepDailySummary(Base):
    __tablename__ = 'sleep_daily_summary'

    date = Column(DateTime, primary_key=True)
    totalMinutesAsleep = Column(Integer)
    totalTimeInBed = Column(Integer)
    deepMinutes = Column(Integer)
    remMinutes = Column(Integer)
    lightMinutes = Column(Integer)
    wakeMinutes = Column(Integer)
    totalSleepRecords = Column(Integer)
    sleepBreakTimes = Column(String(100))
