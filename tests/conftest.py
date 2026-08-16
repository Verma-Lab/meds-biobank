import pytest
from pyspark.sql import SparkSession
from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("meds-biobank-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

### STANDARD OMOP DATA ###

@pytest.fixture(scope="session")
def data_dir():
    return Path(__file__).resolve().parent.parent / os.environ["OMOP_DATA_DIR"]

@pytest.fixture(scope="session")
def concept_ancestor(spark, data_dir):
    return spark.read.csv(str(data_dir / "concept_ancestor.csv"), header=True, inferSchema=True)

@pytest.fixture(scope="session")
def concept(spark, data_dir):
    return spark.read.csv(str(data_dir / "concept.csv"), header=True, inferSchema=True)

@pytest.fixture(scope="session")
def condition_occurrence(spark, data_dir):
    return spark.read.csv(str(data_dir / "condition_occurrence.csv"), header=True, inferSchema=True)

@pytest.fixture(scope="session")
def death(spark, data_dir):
    return spark.read.csv(str(data_dir / "death.csv"), header=True, inferSchema=True)

@pytest.fixture(scope="session")
def drug_exposure(spark, data_dir):
    return spark.read.csv(str(data_dir / "drug_exposure.csv"), header=True, inferSchema=True)

@pytest.fixture(scope="session")
def measurement(spark, data_dir):
    return spark.read.csv(str(data_dir / "measurement.csv"), header=True, inferSchema=True)

@pytest.fixture(scope="session")
def observation(spark, data_dir):
    return spark.read.csv(str(data_dir / "observation.csv"), header=True, inferSchema=True)

@pytest.fixture(scope="session")
def person(spark, data_dir):
    return spark.read.csv(str(data_dir / "person.csv"), header=True, inferSchema=True)

@pytest.fixture(scope="session")
def procedure_occurrence(spark, data_dir):
    return spark.read.csv(str(data_dir / "procedure_occurrence.csv"), header=True, inferSchema=True)

@pytest.fixture(scope="session")
def visit_occurrence(spark, data_dir):
    return spark.read.csv(str(data_dir / "visit_occurrence.csv"), header=True, inferSchema=True)

### PMBB OMOP DATA ###

@pytest.fixture(scope="session")
def pmbb_data_dir():
    return Path(__file__).resolve().parent.parent / os.environ["PMBB_OMOP_DATA_DIR"]

@pytest.fixture(scope="session")
def pmbb_concept_ancestor(spark, pmbb_data_dir):
    return spark.read.csv(str(pmbb_data_dir / "concept_ancestor.csv"), header=True, inferSchema=True)

@pytest.fixture(scope="session")
def pmbb_concept(spark, pmbb_data_dir):
    return spark.read.csv(str(pmbb_data_dir / "concept.csv"), header=True, inferSchema=True)

@pytest.fixture(scope="session")
def pmbb_condition_occurrence(spark, pmbb_data_dir):
    return spark.read.csv(str(pmbb_data_dir / "condition_occurrence.csv"), header=True, inferSchema=True)

@pytest.fixture(scope="session")
def pmbb_death(spark, pmbb_data_dir):
    return spark.read.csv(str(pmbb_data_dir / "death.csv"), header=True, inferSchema=True)

@pytest.fixture(scope="session")
def pmbb_drug_exposure(spark, pmbb_data_dir):
    return spark.read.csv(str(pmbb_data_dir / "drug_exposure.csv"), header=True, inferSchema=True)

@pytest.fixture(scope="session")
def pmbb_measurement(spark, pmbb_data_dir):
    return spark.read.csv(str(pmbb_data_dir / "measurement.csv"), header=True, inferSchema=True)

@pytest.fixture(scope="session")
def pmbb_observation(spark, pmbb_data_dir):
    return spark.read.csv(str(pmbb_data_dir / "observation.csv"), header=True, inferSchema=True)

@pytest.fixture(scope="session")
def pmbb_person(spark, pmbb_data_dir):
    return spark.read.csv(str(pmbb_data_dir / "person.csv"), header=True, inferSchema=True)

@pytest.fixture(scope="session")
def pmbb_procedure_occurrence(spark, pmbb_data_dir):
    return spark.read.csv(str(pmbb_data_dir / "procedure_occurrence.csv"), header=True, inferSchema=True)

@pytest.fixture(scope="session")
def pmbb_visit_occurrence(spark, pmbb_data_dir):
    return spark.read.csv(str(pmbb_data_dir / "visit_occurrence.csv"), header=True, inferSchema=True)

@pytest.fixture(scope="session")
def pmbb_labs(spark, pmbb_data_dir):
    return [
        spark.read.csv(str(pmbb_data_dir / "labs_creatinine.csv"), header=True, inferSchema=True),
        spark.read.csv(str(pmbb_data_dir / "labs_glucose.csv"), header=True, inferSchema=True),
        spark.read.csv(str(pmbb_data_dir / "labs_hba1c.csv"), header=True, inferSchema=True)
    ]

@pytest.fixture(scope="session")
def pmbb_vitals(spark, pmbb_data_dir):
    return [
        spark.read.csv(str(pmbb_data_dir / "vitals_heart_rate.csv"), header=True, inferSchema=True),
        spark.read.csv(str(pmbb_data_dir / "vitals_spo2.csv"), header=True, inferSchema=True),
        spark.read.csv(str(pmbb_data_dir / "vitals_systolic_bp.csv"), header=True, inferSchema=True)
    ]

### POST-ETL EVENTS DATA ###

@pytest.fixture(scope="session")
def pmbb_meds_data_path(spark):
    return Path(__file__).resolve().parent.parent / os.environ["MEDS_DATA_DIR"] / "pmbb_meds.csv"

@pytest.fixture(scope="session")
def pmbb_meds_events(spark, pmbb_meds_data_path):
    return spark.read.csv(str(pmbb_meds_data_path), header=True, inferSchema=True)

@pytest.fixture(scope="session")
def meds_data_path(spark):
    return Path(__file__).resolve().parent.parent / os.environ["MEDS_DATA_DIR"] / "meds.csv"

@pytest.fixture(scope="session")
def meds_events(spark, meds_data_path):
    return spark.read.csv(str(meds_data_path), header=True, inferSchema=True)