from fastapi import FastAPI, HTTPException, Query, Depends
from sqlalchemy import create_engine, Column, Text, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.sql import func, and_
from sqlalchemy import create_engine, Column, Text, Float, BigInteger, TIMESTAMP
from pydantic import BaseModel
import math
from typing import Optional, List
from datetime import datetime

# Database configuration
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Fetch DATABASE_URL from the environment
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL is not set in the environment or .env file")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

# Create FastAPI app
app = FastAPI(title="CRISH API", description="Sample API to fetch case reports and weather parameter forecasts from PostgreSQL", version="1.0")

# Define the case_reports table model
class CaseReport(Base):
    __tablename__ = "case_reports"
    id = Column(BigInteger, primary_key=True, index=True)
    caseType = Column(Text)
    numberOfCases = Column(Float)
    latitude = Column(Float)
    longitude = Column(Float)
    weekNumber = Column(BigInteger)
    fromDateTime = Column(TIMESTAMP)
    toDateTime = Column(TIMESTAMP)
    reportingDate = Column(TIMESTAMP)
    reportingEntityType = Column(Text)
    reportingEntityIdentifier = Column(Float)
    sexGroupMaleCases = Column(Float)
    sexGroupFemaleCases = Column(Float)
    sexGroupUnknownCases = Column(Float)
    ageGroup0To4Cases = Column(Float)
    ageGroup5To18Cases = Column(Float)
    ageGroup19To59Cases = Column(Float)
    ageGroup60PlusCases = Column(Float)
    ageGroupUnknownCases = Column(Float)
    administrativeLevel = Column(BigInteger)

# Database models for the tables
class RainfallDailyWeightedAverage(Base):
    __tablename__ = "rainfall_daily_weighted_average"
    forecast_date = Column(Text, primary_key=True)
    day_name = Column(Text)
    value = Column(Float)
    municipality_code = Column(Text, primary_key=True)
    municipality_name = Column(Text)

class RelativeHumidityDailyAverage(Base):
    __tablename__ = "rh_daily_avg_region"
    forecast_date = Column(Text, primary_key=True)
    day_name = Column(Text)
    value = Column(Float)
    municipality_code = Column(Text, primary_key=True)
    municipality_name = Column(Text)

class TemperatureDailyMax(Base):
    __tablename__ = "tmax_daily_tmax_region"
    forecast_date = Column(Text, primary_key=True)
    day_name = Column(Text)
    value = Column(Float)
    municipality_code = Column(Text, primary_key=True)
    municipality_name = Column(Text)

class WeatherData(BaseModel):
    forecast_date: str
    day_name: str
    municipality_name: str
    relative_humidity: float | None
    temperature_max: float | None
    rainfall: float | None
    heat_index: float | None

class CaseReportModel(BaseModel):
    id: int
    caseType: str
    numberOfCases: Optional[float]
    latitude: Optional[float]
    longitude: Optional[float]
    weekNumber: Optional[int]
    fromDateTime: Optional[datetime]
    toDateTime: Optional[datetime]
    reportingDate: Optional[datetime]
    reportingEntityType: Optional[str]
    reportingEntityIdentifier: Optional[float]
    sexGroupMaleCases: Optional[float]
    sexGroupFemaleCases: Optional[float]
    sexGroupUnknownCases: Optional[float]
    ageGroup0To4Cases: Optional[float]
    ageGroup5To18Cases: Optional[float]
    ageGroup19To59Cases: Optional[float]
    ageGroup60PlusCases: Optional[float]
    ageGroupUnknownCases: Optional[float]
    administrativeLevel: Optional[int]

    class Config:
        orm_mode = True

# Dependency for DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Initialize database
@app.on_event("startup")
def startup_event():
    Base.metadata.create_all(bind=engine)

@app.get(
    "/case_reports",
    summary="Fetch all case reports with filtering and paging",
    tags=["case_reports"],
    response_model=List[CaseReportModel],  # Response will be a list of Pydantic models
)
def get_case_reports(
    db: Session = Depends(get_db),
    caseType: str | None = Query(None, description="Filter by case type"),
    reportingEntityType: str | None = Query(None, description="Filter by reporting entity type"),
    weekNumber: int | None = Query(None, description="Filter by week number"),
    page: int = Query(1, ge=1, description="Page number (starts at 1)"),
    page_size: int = Query(10, ge=1, le=100, description="Number of items per page (max 100)"),
):
    """
    Fetch all case reports from the database with optional filters and paging.
    """
    query = db.query(CaseReport)

    # Apply filters if provided
    if caseType:
        query = query.filter(CaseReport.caseType == caseType)
    if reportingEntityType:
        query = query.filter(CaseReport.reportingEntityType == reportingEntityType)
    if weekNumber:
        query = query.filter(CaseReport.weekNumber == weekNumber)

    # Calculate pagination
    total_records = query.count()  # Total number of records after applying filters
    offset = (page - 1) * page_size  # Calculate offset
    query = query.offset(offset).limit(page_size)  # Apply limit and offset for paging

    case_reports = query.all()

    if not case_reports:
        raise HTTPException(status_code=404, detail="No case reports found")

    return case_reports  # orm_mode in Pydantic handles the serialization

@app.get(
    "/case_reports/{report_id}",
    summary="Fetch a specific case report by ID",
    tags=["case_reports"],
    response_model=CaseReportModel,  # Use the Pydantic model for the response
)
def get_case_report(report_id: int, db: Session = Depends(get_db)):
    """
    Fetch a specific case report by its ID.
    """
    report = db.query(CaseReport).filter(CaseReport.id == report_id).first()
    if not report:
        raise HTTPException(status_code=404, detail="Case report not found")
    return report  # Pydantic model with `orm_mode` will handle serialization

# Heat index computation (basic formula)
def compute_heat_index(temperature: float, humidity: float) -> float | None:
    if temperature is None or humidity is None:
        return None

    # Example formula for heat index (simplified)
    try:
        heat_index = temperature + 0.5555 * (6.11 * math.exp((5417.7530 * (1 / 273.15 - 1 / (273.15 + temperature)))) - 10)
        return heat_index
    except Exception as e:
        return None  # Return None in case of any computation errors

# Endpoint for fetching data
@app.get("/weather", response_model=list[WeatherData], summary="Fetch weather data with computed heat indices", tags=["weather"])
def fetch_weather_data(
    forecast_date: str | None = Query(None, description="Filter by forecast date (YYYY-MM-DD)"),
    municipality_code: str | None = Query(None, description="Filter by municipality code"),
    municipality_name: str | None = Query(None, description="Filter by municipality name"),
    db: Session = Depends(get_db),
):
    query = db.query(
        RelativeHumidityDailyAverage.forecast_date.label("forecast_date"),
        RelativeHumidityDailyAverage.day_name.label("day_name"),
        RelativeHumidityDailyAverage.municipality_name.label("municipality_name"),
        RelativeHumidityDailyAverage.value.label("relative_humidity"),
        TemperatureDailyMax.value.label("temperature_max"),
        RainfallDailyWeightedAverage.value.label("rainfall"),
    ).join(
        TemperatureDailyMax,
        and_(
            RelativeHumidityDailyAverage.forecast_date == TemperatureDailyMax.forecast_date,
            RelativeHumidityDailyAverage.day_name == TemperatureDailyMax.day_name,
            RelativeHumidityDailyAverage.municipality_code == TemperatureDailyMax.municipality_code,
        ),
        isouter=True,
    ).join(
        RainfallDailyWeightedAverage,
        and_(
            RelativeHumidityDailyAverage.forecast_date == RainfallDailyWeightedAverage.forecast_date,
            RelativeHumidityDailyAverage.day_name == RainfallDailyWeightedAverage.day_name,
            RelativeHumidityDailyAverage.municipality_code == RainfallDailyWeightedAverage.municipality_code,
        ),
        isouter=True,
    )

    if forecast_date:
        query = query.filter(RelativeHumidityDailyAverage.forecast_date == forecast_date)
    if municipality_code:
        query = query.filter(RelativeHumidityDailyAverage.municipality_code == municipality_code)
    if municipality_name:
        query = query.filter(RelativeHumidityDailyAverage.municipality_name == municipality_name)

    results = query.all()
    if not results:
        raise HTTPException(status_code=404, detail="No data found for the given filters")

    weather_data = []
    for row in results:
        row_dict = row._mapping
        heat_index = compute_heat_index(row_dict["temperature_max"], row_dict["relative_humidity"])
        print(heat_index)
        weather_data.append(WeatherData(
            forecast_date=row_dict["forecast_date"],
            day_name=row_dict["day_name"],
            municipality_name=row_dict["municipality_name"],
            relative_humidity=row_dict["relative_humidity"],
            temperature_max=row_dict["temperature_max"],
            rainfall=row_dict["rainfall"],
            heat_index=heat_index,
        ))

    return weather_data