from flask_wtf import FlaskForm
from wtforms import DateField, SelectField
from wtforms.validators import DataRequired


class CreateAnomalyForm(FlaskForm):
    warehouse_name = SelectField("warehouse_name", validators=[DataRequired()])
    usage_date = DateField("usage_date", validators=[DataRequired()])
