from meds_biobank.ontologies.ontologies import Ontology
from dotenv import load_dotenv
from pathlib import Path
import os

def test_ontology_builder(spark, pmbb_concept, pmbb_concept_ancestor, pmbb_meds_events):

    # register ontology dirname
    load_dotenv()
    REPO_ROOT = Path(__file__).resolve().parents[2]
    dirname = REPO_ROOT / os.environ["ONTOLOGY_DATA_DIR"]

    # create ontology object, fit, and save
    ontology = Ontology()
    ontology.compute_concept_ontology(pmbb_meds_events, pmbb_concept, pmbb_concept_ancestor)
    ontology.bin_measurements(pmbb_meds_events)
    ontology.rollup_concepts(pmbb_meds_events, pmbb_concept_ancestor)