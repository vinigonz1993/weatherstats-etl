from extract import Extract
from transform import Transform
from load import Load
from connection import initialize_db

initialize_db()


Extract().download()

df = Extract().df()
df = Transform(df).clean_data()

Load(df)