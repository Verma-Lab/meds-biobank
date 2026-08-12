# Standardizers

## Specification

- Guarantee: Each measurement_concept_id corresponds to at most one unit_converted
- Guarantee: Each measurement_id from the input dataframe is still present (potentially duplicated)
- Note: Value_converted potentially contains both numeric and textual entries

## Workflow

1. Homogenize units (adjust values) within designated groups of measurement concept ids (by OMOP concept_ancestor or by OMOP concept_id)
2. Perform fallback logic for other measurement types (current: set = None)
3. Union all results and return edited msmt df
NOTE: measurement_id will no longer be unique! This is intended. NEVER de-deuplicate on measurement_id POST-standardization.

## Schemas

### OMOP measurements

Inputs: measurements.

```bash
measurement_id:long
person_id:string
measurement_concept_id:integer
measurement_date:date
measurement_datetime:timestamp
measurement_type_concept_id:integer
operator_concept_id:integer
value_as_number:decimal(10,3)
value_as_concept_id:integer
unit_concept_id:integer
range_low:decimal(24,3)
range_high:decimal(24,3)
visit_occurrence_id:long
measurement_source_value:string
measurement_source_concept_id:integer
unit_source_value:string
value_source_value:string
```

### Standardized OMOP Measurements

Outputs: standardized measurements.

```bash
# Same fields as above ...

# ADDED:
unit_converted:string
value_converted:object
std_concept_id:long
```

## Covered Measurements

NOTE: Exact Match Codes contains meausrement_concept_id unless otherwise specified in Additional Conditions

### Original

| Group | Status | Parent Codes | Exact Match Codes | Additional Conditions | Abbreviation |
| --- | --- | --- | --- | --- | --- |
| Laboratory Alanine Aminotransferase | labs | 40652525 | — | — | labs_alt |
| Laboratory Albumin | labs | 40652534 | — | — | labs_albumin |
| Laboratory Alkaline Phosphatase | labs | 40652549 | — | — | labs_alp |
| Laboratory Anion Gap | labs | 40652611 | — | — | labs_anion_gap |
| Laboratory Apolipoprotein B | labs | 40652616 | — | — | labs_apo_b |
| Laboratory Mullerian Inhibiting Substance (AMH) | labs | 40653357 | — | — | labs_amh |
| Laboratory Aspartate Aminotransferase | labs | 40652640 | — | — | labs_ast |
| Laboratory Basophils | labs | 40653984 | — | — | labs_basophils |
| Laboratory Beta Globulin | labs | 1990626 | — | — | labs_beta_globulin |
| Laboratory Bilirubin, Total | labs | 40652709 | — | — | labs_bilirubin_total |
| Laboratory Urea Nitrogen (BUN) | labs | 40653900 | — | — | labs_bun |
| Laboratory C-Reactive Protein | labs | 40652733 | 3020460 | — | labs_crp |
| Laboratory C-Reactive Protein, High Sensitivity | labs | 40654479 | — | — | labs_crp_hs |
| Laboratory Calcium | labs | 40652745 | — | — | labs_calcium |
| Laboratory Chloride | labs | 40652796 | — | — | labs_chloride |
| Laboratory Cholesterol in HDL | labs | 40652802 | — | — | labs_chol_hdl |
| Laboratory Cholesterol in LDL | labs | 40654572 | — | — | labs_chol_ldl |
| Laboratory Cholesterol in VLDL | labs | 40654576 | 3009596 | — | labs_chol_vldl |
| Laboratory Cholesterol, Total | labs | 40652808 | — | — | labs_chol_total |
| Laboratory Complement C4 | labs | 40654637 | — | — | labs_c4 |
| Laboratory SARS-CoV-2 Detection | labs | 36662633 | — | — | labs_covid |
| Laboratory Creatine Kinase-MB | labs | 40652867 | — | — | labs_ck |
| Laboratory Creatinine | labs | 40652870 | — | — | labs_creatinine |
| Laboratory Creatinine, Urine | labs | 40656057 | — | — | labs_creatinine_urine |
| Laboratory Cyclic Citrullinated Peptide Antibody | labs | 40652885 | — | — | labs_ccpab |
| Laboratory Eosinophils | labs | 40653994 | — | — | labs_eosinophils |
| Laboratory Erythrocyte Sedimentation Rate (ESR) | labs | 4028908 | — | concept_name like '%eryth%' and like '%sed%' | labs_esr |
| Laboratory Erythrocytes (RBC) | labs | 40654005 | — | — | labs_rbc |
| Laboratory Erythrocytes, Urine | labs | 40657685 | — | — | labs_rbc_urine |
| Laboratory Ferritin | labs | 40652982 | — | — | labs_ferritin |
| Laboratory Folate | labs | 40652995 | — | — | labs_folate |
| Laboratory Gamma-Glutamyl Transferase (GGT) | labs | — | — | concept_name like '%glutamyl%transferase%' (no ancestor filter at all) | labs_ggt |
| Laboratory Glucose | labs | 40653085 | — | value_source_value not like '%,%' | labs_glucose |
| Laboratory Glucose, Fasting | labs | — | 3037110 | matched via measurement_source_concept_id, not measurement_concept_id | labs_glucose_fasting |
| Laboratory Glucose, Urine | labs | 40657691 | — | — | labs_glucose_urine |
| Laboratory Granulocytes | labs | 40654016 | — | — | labs_granulocytes |
| Laboratory Hemoglobin | labs | 40654905 | — | — | labs_hemoglobin |
| Laboratory Hemoglobin A1c | labs | 1621295 | — | — | labs_hba1c |
| Laboratory Immunoglobulin A (IgA) | labs | 40653141 | — | — | labs_iga |
| Laboratory Immunoglobulin G (IgG) | labs | 40653151 | — | — | labs_igg |
| Laboratory Immunoglobulin M (IgM) | labs | 40653158 | — | — | labs_igm |
| Laboratory International Normalized Ratio (INR) | labs | — | — | concept_name rlike '.*\binr\b.*' (no ancestor filter at all) | labs_inr |
| Laboratory Iron | labs | 40654984 | — | — | labs_iron |
| Laboratory Ketones, Urine | labs | 40656264 | "5797-6" | matched via measurement_source_value, not measurement_concept_id | labs_ketones |
| Laboratory Leukocyte Esterase, Urine | labs | 40657703 | — | — | labs_leukocyte_esterase |
| Laboratory Leukocytes (WBC) | labs | 40654026 | — | post-processed: deduped against labs_lymphocytes on (person_id, measurement_date, value_source_value) | labs_wbc |
| Laboratory Leukocytes, Urine | labs | 40657704 | — | — | labs_wbc_urine |
| Laboratory Lipoprotein(a) | labs | 40655033 | — | — | labs_lipoprotein_a |
| Laboratory Lymphocytes | labs | 40654045 | — | — | labs_lymphocytes |
| Laboratory Magnesium | labs | 40653291 | — | — | labs_magnesium |
| Laboratory Metamyelocytes | labs | 40654064 | 3012392, 3024507 | — | labs_metamyelocytes |
| Laboratory Metanephrine | labs | 40655090 | — | — | labs_metanephrine |
| Laboratory Microalbumin, Urine | labs | 40656529 | — | — | labs_microalbumin |
| Laboratory Microalbumin/Creatinine Ratio, Urine | labs | 40656531 | — | — | labs_microalbumin_creatinine_ratio |
| Laboratory Monocytes | labs | 40654069 | — | — | labs_monocytes |
| Laboratory Neutrophils | labs | 40654088 | — | — | labs_neutrophils |
| Laboratory Neutrophils, Band Form | labs | 40654083 | 3008939, 3018199 | — | labs_neutrophils_bandform |
| Laboratory Neutrophils, Segmented | labs | 40654086 | 3027270, 3015586 | — | labs_neutrophils_segmented |
| Laboratory Normetanephrine | labs | 40655204 | — | — | labs_normetanephrine |
| Laboratory Partial Thromboplastin Time (PTT) | labs | 1618784, 2212742 | — | concept_name like '%ptt%' | labs_ptt |
| Laboratory Phosphate | labs | 40653550 | — | — | labs_phosphate |
| Laboratory Platelets | labs | 40654106 | — | — | labs_platelets |
| Laboratory Potassium | labs | 40653596 | — | — | labs_potassium |
| Laboratory Prealbumin | labs | 40653598 | — | — | labs_prealbumin |
| Laboratory Promyelocytes | labs | 40654115 | 3022709, 3024153 | — | labs_promyelocytes |
| Laboratory Protein, Total | labs | 40653626 | — | — | labs_protein |
| Laboratory Protein, Urine | labs | 40657714 | 3005897, 3035511, 3014051, 3037121, 40760845 | — | labs_protein_urine |
| Laboratory Prothrombin Time (PT) | labs | — | 3034426, 2212731 | — | labs_pt |
| Laboratory Rheumatoid Factor | labs | 40653663 | 3021614, 3015688, 3024763 | — | labs_rf |
| Laboratory Sex Hormone Binding Globulin (SHBG) | labs | 40655429 | — | — | labs_shbg |
| Laboratory Sodium | labs | 40653762 | — | — | labs_sodium |
| Laboratory Testosterone, Free | labs | 40653808 | — | — | labs_testosterone_free |
| Laboratory Thyrotropin (TSH) | labs | 40653836 | — | — | labs_tsh |
| Laboratory Transferrin | labs | 1990650 | — | — | labs_transferrin |
| Laboratory Triglycerides | labs | 40653862 | — | — | labs_triglycerides |
| Laboratory Troponin I, Cardiac | labs | 40653873 | — | — | labs_troponin_i |
| Laboratory Troponin T, Cardiac | labs | 40653874 | — | — | labs_troponin_t |
| Laboratory Urobilinogen, Urine | labs | 40656506 | — | — | labs_urobilinogen |
| Vital Blood Pressure, Diastolic | vitals | — | 3012888 | — | vitals_bp_diastolic |
| Vital Blood Pressure, Systolic | vitals | — | 3004249 | — | vitals_bp_systolic |
| Vital Body Mass Index (BMI) | vitals | — | 44783982, 4245997 | — | vitals_bmi |
| Vital Body Height | vitals | 40655804 | — | — | vitals_height |
| Vital Oxygen Saturation | vitals | 40654168 | — | — | vitals_o2sat |
| Vital Heart Rate (Pulse) | vitals | 40654164 | — | — | vitals_pulse |
| Vital Respiratory Rate | vitals | 40654163 | — | — | vitals_resp_rate |
| Vital Body Temperature | vitals | 40654162 | — | — | vitals_temperature |
| Vital Body Weight | vitals | 40655805 | — | — | vitals_weight |

### Added

| Group | Status | Parent Codes | Exact Match Codes | Additional Conditions | Abbreviation |
| --- | --- | --- | --- | --- | --- |
| - | - | - | — | - | - |
