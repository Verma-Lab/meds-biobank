# meds_biobank.standardizers

Homogenizes OMOP `measurement` rows so that every occurrence of "the same" lab or vital ends up under one concept id, with values converted to one unit per concept. Two independent implementations exist in the same module:

| | Status | Shape |
| --- | --- | --- |
| [`standardize_measurement_concept_id`](#standardize_measurement_concept_idmeasurement-concept-concept_ancestor) + [`standardize_numeric_values_and_units`](#standardize_numeric_values_and_unitsmeasurement-mcid_standardizedtrue) + [`standardize_text_value`](#standardize_text_valuemeasurement) | **Live** — used by `omop_meds.py` and the test suite | Three composable functions, driven by a declarative `GROUPINGS` table. |
| [`lv_standardize`](#lv_standardizemsmt-concept-concept_ancestor) | **Legacy / unused** — not imported anywhere else in the codebase | One large function, ~90 hand-written per-lab/vital blocks doing concept-id mapping and unit conversion together. |

**Source:** `src/meds_biobank/standardizers/standardizers.py`

---

## Schemas

### Input: OMOP measurements

`schemas.OMOP_MEASUREMENT_SCHEMA`

```
measurement_id:long                     person_id:string
measurement_concept_id:integer          measurement_date:date
measurement_datetime:timestamp          measurement_type_concept_id:integer
operator_concept_id:integer             value_as_number:double
value_as_concept_id:integer             unit_concept_id:integer
range_low:double                        range_high:double
visit_occurrence_id:integer             measurement_source_value:string
measurement_source_concept_id:integer   unit_source_value:string
unit_source_concept_id:integer          value_source_value:string
```

### Output: standardized OMOP measurements

`schemas.STD_OMOP_MEASUREMENT_SCHEMA` — every input field, plus:

```
concept_id_std:integer      # standardized concept id (from standardize_measurement_concept_id)
numeric_value_std:double    # standardized numeric value (from standardize_numeric_values_and_units)
unit_std:string             # standardized unit (from standardize_numeric_values_and_units)
text_value_std:string       # cleaned text value (from standardize_text_value)
```

- **Guarantee:** every `measurement_id` present in the input is still present in the output (potentially duplicated — see below).
- **Guarantee:** each `concept_id_std` maps to at most one `unit_std` (its per-concept modal unit; see [Unit conversion](#unit-conversion)).
- **Note:** `measurement_id` is **not** unique after `standardize_measurement_concept_id` — a row matching more than one `GROUPINGS` entry appears once per match. Never deduplicate on `measurement_id` after standardization.

---

## `standardize_measurement_concept_id(measurement, concept, concept_ancestor)`

Computes each measurement's standardized concept id by applying every entry of [`GROUPINGS`](#groupings-reference) in turn.

**Parameters**

| Name | Type | Description |
| --- | --- | --- |
| `measurement` | `pyspark.sql.DataFrame` | OMOP `measurement` table (`OMOP_MEASUREMENT_SCHEMA`). |
| `concept` | `pyspark.sql.DataFrame` | OMOP `concept` table (`OMOP_CONCEPT_SCHEMA`). |
| `concept_ancestor` | `pyspark.sql.DataFrame` | OMOP `concept_ancestor` table (`OMOP_CONCEPT_ANCESTOR_SCHEMA`). |

**Returns:** `measurement`, with one new column `concept_id_std`. `measurement_concept_id` itself is untouched.

**Behavior:** left-joins `measurement` to `concept_ancestor`/`concept` once, then for every `GROUPINGS` entry, filters that joined base to the entry's predicate, de-duplicates on `measurement_id` (guards against the ancestor join's fan-out), and stamps `concept_id_std = std_concept_id`. All matching rows across all entries are unioned. Any row matching **no** entry passes through unioned in too, with `concept_id_std` left equal to its own `measurement_concept_id`. A row matching more than one entry's predicate is duplicated — once per match.

**Raises:** `ValueError` if any argument isn't a `DataFrame`, or doesn't match its expected schema.

---

## `standardize_numeric_values_and_units(measurement, mcid_standardized=True)`

Homogenizes each measurement's unit (and converts its value to match) within groups of the same standardized concept id.

**Parameters**

| Name | Type | Default | Description |
| --- | --- | --- | --- |
| `measurement` | `pyspark.sql.DataFrame` | — | Typically the output of `standardize_measurement_concept_id`. |
| `mcid_standardized` | `bool` | `True` | If `True`, groups by `concept_id_std`; if `False`, groups by the raw `measurement_concept_id` instead (used as the fallback path for measurements `standardize_measurement_concept_id` didn't touch). |

**Returns:** `measurement`, with two new columns `numeric_value_std` and `unit_std`. `value_as_number` / `unit_source_value` are untouched.

### Unit conversion

1. Every `unit_source_value` is mapped to one of ~30 fixed normalized-unit labels (`unit_norm`) via a large pattern ladder (e.g. `mg/dL`, `g/dL`, `g/L`, `ng/mL`, `Thousand/uL`, `mmol/L`, `mmHg`, `Second(s)`, `%`, …). Anything unrecognized keeps its original string.
2. **The target unit is per-concept, not fixed:** for each `(concept_id_std or measurement_concept_id)` group, the *most common* `unit_norm` observed for that concept becomes that concept's own target unit (ties broken alphabetically by `unit_norm`). Two different concepts in the same unit family can therefore end up with two different target units.
3. Every row is converted from its own `unit_norm` to its concept's target unit using a shared family-relative conversion-factor table (e.g. within the mass-concentration family, `mg/dL=1.0, g/dL=1000.0, g/L=0.01, mg/L=0.1, mg/mL=100.0`). `numeric_value_std` and `unit_std` are only set when both the source and target units are in that table (or are already identical) — otherwise both stay `null`.

> **Known gap:** the pattern ladder explicitly excludes **temperature** (`vitals_temperature`) — its comment says so directly ("temperature intentionally excluded"). Temperature rows always get `numeric_value_std = null` / `unit_std = null` through this function, unlike the legacy `lv_standardize` (below), which does convert °C → °F.

**Raises:** `ValueError` if `measurement` isn't a `DataFrame`.

---

## `standardize_text_value(measurement)`

Cleans up non-numeric measurement values.

**Parameters**

| Name | Type | Description |
| --- | --- | --- |
| `measurement` | `pyspark.sql.DataFrame` | Any measurement table with a `value_source_value` column. |

**Returns:** `measurement`, with one new column `text_value_std`: for every row where `value_source_value` is non-null and **doesn't** parse as a `DOUBLE`, its whitespace is trimmed, internal whitespace runs collapsed to a single space, and the result lowercased (so `"POSITIVE"`, `"  positive"`, and `"Positive  "` all become `"positive"`). Rows with a numeric or null `value_source_value` get `text_value_std = null`. `value_source_value` itself is untouched.

---

## GROUPINGS reference

The declarative table `standardizers.GROUPINGS` — a list of `(label, predicate, std_concept_id)` tuples — that `standardize_measurement_concept_id` iterates over. "Parent" means the predicate matches via `concept_ancestor.ancestor_concept_id`; "Exact" means it matches specific `measurement_concept_id`s directly.

| Group | Parent (ancestor id) | Exact match id(s) | Additional condition | → `std_concept_id` |
| --- | --- | --- | --- | --- |
| labs_alt | 40652525 | — | — | 40652525 |
| labs_albumin | 40652534 | — | — | 40652534 |
| labs_alp | 40652549 | — | — | 40652549 |
| labs_anion_gap | 40652611 | — | — | 40652611 |
| labs_apo_b | 40652616 | — | — | 40652616 |
| labs_amh | 40653357 | — | — | 40653357 |
| labs_ast | 40652640 | — | — | 40652640 |
| labs_basophils | 40653984 | — | — | 40653984 |
| labs_beta_globulin | 1990626 | — | — | 1990626 |
| labs_bilirubin_total | 40652709 | — | — | 40652709 |
| labs_bun | 40653900 | — | — | 40653900 |
| labs_crp | 40652733 | 3020460 | — | 40652733 |
| labs_crp_hs | 40654479 | — | — | 40654479 |
| labs_calcium | 40652745 | — | — | 40652745 |
| labs_chloride | 40652796 | — | — | 40652796 |
| labs_chol_hdl | 40652802 | — | — | 40652802 |
| labs_chol_ldl | 40654572 | — | — | 40654572 |
| labs_chol_vldl | 40654576 | 3009596 | — | 40654576 |
| labs_chol_total | 40652808 | — | — | 40652808 |
| labs_c4 | 40654637 | — | — | 40654637 |
| labs_covid | 36662633 | — | — | 36662633 |
| labs_ck | 40652867 | — | — | 40652867 |
| labs_creatinine | 40652870 | — | — | 40652870 |
| labs_creatinine_urine | 40656057 | — | — | 40656057 |
| labs_ccpab | 40652885 | — | — | 40652885 |
| labs_eosinophils | 40653994 | — | — | 40653994 |
| labs_rbc | 40654005 | — | — | 40654005 |
| labs_rbc_urine | 40657685 | — | — | 40657685 |
| labs_esr | 4028908 | — | `concept_name` matches `%eryth%` and `%sed%` | 3015183 |
| labs_ggt | — | — | `concept_name` like `%glutamyl%transferase%` (no ancestor filter at all) | 2212371 |
| labs_ferritin | 40652982 | — | — | 40652982 |
| labs_folate | 40652995 | — | — | 40652995 |
| labs_glucose | 40653085 | — | `value_source_value` not like `%,%` | 40653085 |
| labs_glucose_fasting | — | — | matched via `measurement_source_concept_id == 3037110` | 3037110 |
| labs_glucose_urine | 40657691 | — | — | 40657691 |
| labs_granulocytes | 40654016 | — | — | 40654016 |
| labs_hemoglobin | 40654905 | — | — | 40654905 |
| labs_hba1c | 1621295 | — | — | 1621295 |
| labs_iga | 40653141 | — | — | 40653141 |
| labs_igg | 40653151 | — | — | 40653151 |
| labs_igm | 40653158 | — | — | 40653158 |
| labs_inr | — | — | `concept_name` rlike `.*\binr\b.*` (no ancestor filter at all) | 85610 |
| labs_iron | 40654984 | — | — | 40654984 |
| labs_ketones | 40656264 | — | or `measurement_source_value == "5797-6"` | 40656264 |
| labs_leukocyte_esterase | 40657703 | — | — | 40657703 |
| labs_wbc | 40654026 | — | — | 40654026 |
| labs_wbc_urine | 40657704 | — | — | 40657704 |
| labs_lipoprotein_a | 40655033 | — | — | 40655033 |
| labs_lymphocytes | 40654045 | — | — | 40654045 |
| labs_magnesium | 40653291 | — | — | 40653291 |
| labs_metamyelocytes | 40654064 | 3012392, 3024507 | — | 40654064 |
| labs_metanephrine | 40655090 | — | — | 40655090 |
| labs_microalbumin | 40656529 | — | — | 40656529 |
| labs_microalbumin_creatinine_ratio | 40656531 | — | — | 40656531 |
| labs_monocytes | 40654069 | — | — | 40654069 |
| labs_neutrophils | 40654088 | — | — | 40654088 |
| labs_neutrophils_bandform | 40654083 | 3008939, 3018199 | — | 40654083 |
| labs_neutrophils_segmented | 40654086 | 3027270, 3015586 | — | 40654086 |
| labs_normetanephrine | 40655204 | — | — | 40655204 |
| labs_ptt | 1618784, 2212742 | — | `concept_name` like `%ptt%` | 2212742 |
| labs_phosphate | 40653550 | — | — | 40653550 |
| labs_platelets | 40654106 | — | — | 40654106 |
| labs_potassium | 40653596 | — | — | 40653596 |
| labs_prealbumin | 40653598 | — | — | 40653598 |
| labs_promyelocytes | 40654115 | 3022709, 3024153 | — | 40654115 |
| labs_protein | 40653626 | — | — | 40653626 |
| labs_protein_urine | 40657714 | 3005897, 3035511, 3014051, 3037121, 40760845 | — | 40657714 |
| labs_pt | — | 3034426, 2212731 | — | 3034426 |
| labs_rf | 40653663 | 3021614, 3015688, 3024763 | — | 40653663 |
| labs_shbg | 40655429 | — | — | 40655429 |
| labs_sodium | 40653762 | — | — | 40653762 |
| labs_testosterone_free | 40653808 | — | — | 40653808 |
| labs_tsh | 40653836 | — | — | 40653836 |
| labs_transferrin | 1990650 | — | — | 1990650 |
| labs_triglycerides | 40653862 | — | — | 40653862 |
| labs_troponin_i | 40653873 | — | — | 40653873 |
| labs_troponin_t | 40653874 | — | — | 40653874 |
| labs_urobilinogen | 40656506 | — | — | 40656506 |
| vitals_bp_diastolic | — | 3012888 | — | 3012888 |
| vitals_bp_systolic | — | 3004249 | — | 3004249 |
| vitals_bmi | — | 44783982, 4245997 | — | 44783982 |
| vitals_height | 40655804 | — | — | 40655804 |
| vitals_o2sat | 40654168 | — | — | 40654168 |
| vitals_pulse | 40654164 | — | — | 40654164 |
| vitals_resp_rate | 40654163 | — | — | 40654163 |
| vitals_temperature | 40654162 | — | — | 40654162 |
| vitals_weight | 40655805 | — | — | 40655805 |

Unlike `lv_standardize` (below), this path does **not** de-duplicate `labs_wbc` against `labs_lymphocytes` — that fixup only exists in the legacy function.

---

## `lv_standardize(msmt, concept, concept_ancestor)`

**Legacy — not called anywhere else in the codebase.** A self-contained, single-function equivalent of `standardize_measurement_concept_id` + `standardize_numeric_values_and_units` combined: for each of ~90 hand-written lab/vital blocks (materially the same coverage as [`GROUPINGS`](#groupings-reference)), it filters, stamps a `std_concept_id`, and computes a converted value/unit for that block, then unions every block together, falling back to `standardize_numeric_values_and_units(..., mcid_standardized=False)` for anything unmapped.

**Parameters**

| Name | Type | Description |
| --- | --- | --- |
| `msmt` | `pyspark.sql.DataFrame` | OMOP `measurement` table, must **exactly** match `OMOP_MEASUREMENT_SCHEMA` (not just ignoring column order, unlike the live functions). |
| `concept` | `pyspark.sql.DataFrame` | OMOP `concept` table. |
| `concept_ancestor` | `pyspark.sql.DataFrame` | OMOP `concept_ancestor` table. |

**Returns:** `final_msmt` (`pyspark.sql.DataFrame`) — every `OMOP_MEASUREMENT_SCHEMA` column plus `concept_id_std`, `numeric_value_std`, `unit_std`.

**Raises:** `ValueError` if any argument isn't a `DataFrame` or doesn't match its schema exactly.

**Behavior, per block:** join `msmt` to `concept_ancestor` (`msmt_ca`), filter to the block's predicate (mirrors the corresponding `GROUPINGS` entry, including the same "no ancestor filter" and `concept_name`-pattern special cases for `labs_esr`, `labs_ggt`, `labs_inr`, `labs_ptt`, `labs_pt`), stamp `std_concept_id`, and compute `value_converted`/`unit_converted` **inline** via a hardcoded `F.when` unit ladder specific to that lab (rather than the shared, per-concept-mode logic `standardize_numeric_values_and_units` uses) — each block picks one fixed target unit and converts (or nulls) accordingly.

**Differences from the live `GROUPINGS` path:**

- **Deduplicates `labs_wbc` against `labs_lymphocytes`:** rows matching `labs_wbc` are dropped via a left-anti join on `(person_id, measurement_date, value_source_value)` against `labs_lymphocytes` — a known data-quality fixup that `standardize_measurement_concept_id` does not perform.
- **Converts temperature:** `vitals_temperature` converts °F/°C to a fixed `"Degrees Fahrenheit"`, unlike the live `standardize_numeric_values_and_units`, which explicitly skips temperature entirely.
- **Fixed per-block target unit**, rather than the live path's per-concept modal-unit logic — e.g. `labs_albumin` always targets `g/dL` here, regardless of what unit is actually most common in the data for that concept.
- Two blocks (`labs_covid`, `labs_ccpab`, `labs_rbc_urine`, `labs_wbc_urine`, `labs_granulocytes`) have no unit-conversion logic at all — `value_converted`/`unit_converted` are always `null`.
- Several `std_concept_id` choices are flagged in-source as semi-arbitrary picks among multiple candidate codes (`labs_alt`'s block is not one of these, but `labs_inr` → CPT4 code `85610`, `labs_ggt` → `2212371`, `labs_ptt` → `2212742`, `labs_pt` → `3034426`, `vitals_bmi` → `44783982` all carry a `# TODO: review` comment in source).

---

## Example

```python
from meds_biobank.standardizers.standardizers import (
    standardize_measurement_concept_id,
    standardize_numeric_values_and_units,
    standardize_text_value,
)

measurement = standardize_measurement_concept_id(measurement, concept, concept_ancestor)
measurement = standardize_numeric_values_and_units(measurement)
measurement = standardize_text_value(measurement)
```
