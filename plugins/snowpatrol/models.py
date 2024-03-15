from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Column, Date, DateTime, Float, Integer, Text
from sqlalchemy.orm import declarative_base

Base = declarative_base()


db = SQLAlchemy(model_class=Base)


class Metering(db.Model):
    __tablename__ = "metering"

    id = Column(Integer, nullable=False, autoincrement=True, primary_key=True)
    warehouse_name = Column(Text, nullable=False, unique=True)
    usage_date = Column(Date, nullable=False, unique=True)
    credits_used = Column(Float, nullable=False)
    credits_used_compute = Column(Float, nullable=False)
    credits_used_cloud_services = Column(Float, nullable=False)
    credits_used_sma30 = Column(Float, nullable=False)
    credits_used_compute_sma30 = Column(Float, nullable=False)
    credits_used_cloud_services_sma30 = Column(Float, nullable=False)
    credits_used_std30 = Column(Float, nullable=False)
    credits_used_compute_std30 = Column(Float, nullable=False)
    credits_used_cloud_services_std30 = Column(Float, nullable=False)

    def __repr__(self):
        return f"<Metering {self.id} {self.warehouse_name} {self.usage_date} {self.credits_used}>"


class Anomaly(db.Model):
    __tablename__ = "anomaly"

    id = Column(Integer, nullable=False, autoincrement=True, primary_key=True)
    warehouse_name = Column(Text, nullable=False, unique=True)
    usage_date = Column(Date, nullable=False, unique=True)
    credits_used = Column(Float, nullable=False)
    trend = Column(Float, nullable=False)
    seasonal = Column(Float, nullable=False)
    residual = Column(Float, nullable=False)
    score = Column(Float, nullable=False)
    prediction_datetime = Column(DateTime, nullable=False)

    def __repr__(self):
        return (
            f"<Anomaly {self.id} {self.warehouse_name} {self.usage_date} {self.score}>"
        )


class Annotation(db.Model):
    __tablename__ = "annotation"

    id = Column(Integer, nullable=False, autoincrement=True, primary_key=True)
    warehouse_name = Column(Text, nullable=False, unique=True)
    usage_date = Column(Date, nullable=False, unique=True)
    annotation_datetime = Column(DateTime, nullable=False)
    # TODO: Add annotated_by field
    comment = Column(Text, nullable=True, default=None)

    def __repr__(self):
        return f"<Annotation {self.id} {self.warehouse_name} {self.usage_date} {self.annotation_datetime}>"
