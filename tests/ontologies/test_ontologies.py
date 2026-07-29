from meds_biobank.ontologies.ontologies import Ontology

def test_ontology_builder(spark, pmbb_concept, pmbb_concept_ancestor, pmbb_meds_events):

    # register ontology dirname
    dirname = "/Users/zolensky/Code/meds-biobank/data/ontologies"

    # create ontology object, fit, and save
    ontology = Ontology()
    ontology.compute_concept_ontology(pmbb_meds_events, pmbb_concept, pmbb_concept_ancestor)
    ontology.bin_measurements(pmbb_meds_events)
    ontology.rollup_concepts(pmbb_meds_events, pmbb_concept_ancestor)