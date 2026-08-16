# PMBB concepts
PMBB_BIRTH = 700000008
PMBB_DEATH = 700000009

# PMBB visit flags
VISIT_FLAGS = {
    "IsHospitalAdmission": 700000001,
    "IsInpatientAdmission": 700000002,
    "IsObservation": 700000003,
    "IsEdVisit": 700000004,
    "IsOutpatientFaceToFaceVisit": 700000005,
    "IsVideoVisit": 700000007,
}

# all custom concepts compiled
SPECIAL_CONCEPTS = VISIT_FLAGS
SPECIAL_CONCEPTS["PMBB_BIRTH"] = PMBB_BIRTH
SPECIAL_CONCEPTS["PMBB_DEATH"] = PMBB_DEATH