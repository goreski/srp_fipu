from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base

# Define the database connection
DATABASE_URL = "mysql+pymysql://root:root@localhost:3306/dw"
engine = create_engine(DATABASE_URL, echo=True)

# Import your models here
from generate_star_schema import Base  # replace 'your_model_file' with the filename (no .py) that contains the class definitions

# Drop all tables defined in Base.metadata
Base.metadata.drop_all(engine)

print("✅ All dimensional model tables dropped.")