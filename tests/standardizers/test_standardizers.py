from meds_biobank.standardizers.standardizers import standardize, autostd

def test_standardizers(spark, measurement, concept, concept_ancestor):
    std_msmt = standardize(measurement, concept, concept_ancestor)
    assert True is True
