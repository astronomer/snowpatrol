# This file contains the model for the AnnotatedAnomaly table in the database.
# Snowflake stores all case-insensitive object names in uppercase text.
# In contrast, SQLAlchemy considers all lowercase object names to be case-insensitive.
# Snowflake SQLAlchemy converts the object name case during schema-level communication.
# https://github.com/snowflakedb/snowflake-sqlalchemy#object-name-case-handling

from sqlalchemy import Boolean, Column, Date, Float, Integer, String, UniqueConstraint
from flask_appbuilder.models.sqla import Model
from sqlalchemy.sql import expression
from airflow.models.base import Base
from plugins.snowsurge import bind_name


Model.metadata = Base.metadata

class AnnotatedAnomaly(Model):
    __bind_key__ = bind_name
    __tablename__ = "annotated_anomalies"
    __table_args__ = (UniqueConstraint("usage_date", "warehouse_name"), {'extend_existing': True})


    id = Column(Integer, primary_key=True, autoincrement=True)
    usage_date = Column(Date, nullable=False)
    warehouse_name = Column(String(20), nullable=False)
    credits_used = Column(Float, nullable=False)
    is_valid = Column(Boolean, nullable=False, default=expression.false())
    comment = Column(String(1000), nullable=True, default=None)

    def __repr__(self):
        return f"<AnnotatedAnomaly {self.usage_date} {self.warehouse_name} {self.credits_used} {self.is_valid} {self.comment}>"