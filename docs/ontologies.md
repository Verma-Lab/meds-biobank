# meds_biobank.ontologies

A single class, `Ontology`, that assembles and stores every symbol table and lookup map needed to tokenize a MEDS event stream: codes, event types, qualifiers, units, decile bin ranges for measurements, common text values, and a rollup map from rare codes to their nearest common ancestor.

**Source:** `src/meds_biobank/ontologies/ontologies.py`

**Requirement:** input events assume each (measurement) code maps to at most one unit.

---

## `Ontology()`

Takes no arguments. Initializes every field (below) to `None`; fields are only populated by calling the `compute_*`/`bin_*`/`rollup_concepts` methods (to build one from scratch) or `load_from_disk` (to load a previously-saved one).

### Attributes

**Symbol sets** — populated by `compute_concept_ontology`, `bin_measurements`, `bin_text_values`

| Name | Type | Description |
| --- | --- | --- |
| `codes` | `Set[int]` | Every OMOP concept id occurring in the data. Can shrink after `rollup_concepts()` (see below). |
| `factors` | `Set[int]` | Every concept id that appears anywhere as a same-domain ancestor of another code. |
| `event_types` | `Set[str]` | Every distinct `event_type` value. |
| `qualifiers` | `Set[str]` \| `None` | Every distinct qualifier value. Stays `None` if no `qualifiers` DataFrame was passed to `compute_concept_ontology`. |
| `bins` | `Set[str]` | Fixed: `{"bin_0", ..., "bin_10"}` (11 decile-bin symbols). |
| `units` | `Set[str]` | Every distinct unit seen among `measurement`-type concepts. |
| `common_text_values` | `Set[str]` | Top-`k` most frequent non-numeric measurement text values. Populated by `bin_text_values`. |

**Code metadata maps** — populated by `compute_concept_ontology`

| Name | Type | Description |
| --- | --- | --- |
| `code_to_event_type` | `Dict[int, str]` | Code → event type. |
| `code_to_name` | `Dict[int, str]` | Code → human-readable concept name. |
| `code_to_qualifiers` | `Dict[int, List[str]]` \| `None` | Code → list of qualifiers. `None` unless a `qualifiers` DataFrame was passed in. |
| `code_to_factors` | `Dict[int, List[int]]` | Code → list of same-domain ancestor concept ids. |
| `code_to_unit` | `Dict[int, List[str]]` | Code → list of units seen for that code. |

**Derived structures** — populated by their own dedicated methods

| Name | Type | Description |
| --- | --- | --- |
| `code_to_bin_ranges` | `Dict[int, Dict[str, Dict[int, Dict[str, float]]]]` | code → unit → decile (1–10) → `{"min": ..., "max": ...}`, in `log1p` space. Populated by `bin_measurements`. |
| `rollup_map` | `Dict[int, int]` | Rare code → nearest same-domain ancestor above the frequency threshold. Populated by `rollup_concepts`. |

---

### `Ontology.compute_concept_ontology(events, concept_schema, qualifiers=None)`

Populates every field listed under "Symbol sets" and "Code metadata maps" above, except `common_text_values` (see `bin_text_values`).

**Parameters**

| Name | Type | Description |
| --- | --- | --- |
| `events` | `pyspark.sql.DataFrame` | **Currently unused** — accepted but never read in the method body. Kept for API symmetry with the other `Ontology` methods. |
| `concept_schema` | `pyspark.sql.DataFrame` | A MEDS `ConceptSchema` DataFrame (see [`etl_pipelines`](./etl_pipelines.md#schemas), columns `code`, `name`, `ancestors`, `factors`, `event_type`, `units`). This is where everything is actually read from. |
| `qualifiers` | `pyspark.sql.DataFrame` \| `None` | Optional, schema `|code|qualifier|source|`. If omitted, `qualifiers` and `code_to_qualifiers` stay `None`. |

**Returns:** `None` (mutates `self` in place).

**Raises:** re-raises whatever exception occurred, after resetting every symbol set and metadata map back to `None`.

---

### `Ontology.bin_measurements(events)`

Computes `code_to_bin_ranges` by bucketing every measurement's numeric value into deciles, per `(code, unit)`. Requires `code_to_event_type` to already be populated (call `compute_concept_ontology` first).

**Parameters**

| Name | Type | Description |
| --- | --- | --- |
| `events` | `pyspark.sql.DataFrame` | Schema `\|patient_id\|code\|time\|end\|numeric_value\|text_value\|unit\|event_type\|visit_id\|`. |

**Returns:** `None` (mutates `self.code_to_bin_ranges` in place).

**Behavior**

1. Restrict to `event_type == "measurement"` rows.
2. Clip negative `numeric_value` to `0`.
3. If a code's values are all identical (homogeneous), null out its `numeric_value` entirely — such a code contributes no usable bin ranges.
4. Drop rows with a null/zero value or a null unit.
5. Apply `log1p` to the remaining values.
6. Bucket into 10 deciles (`ntile(10)`) per `(code, unit)`, ordered by value.
7. Record each `(code, unit, decile)`'s `min`/`max` (in `log1p` space).

**Raises:** re-raises after resetting `code_to_bin_ranges` to `None`.

---

### `Ontology.bin_text_values(events, top_k=100)`

Computes `common_text_values`: the `top_k` most frequent non-numeric measurement text values across the whole dataset (not per-code).

**Parameters**

| Name | Type | Default | Description |
| --- | --- | --- | --- |
| `events` | `pyspark.sql.DataFrame` | — | Must exactly match `schemas.MEDS_EVENT_SCHEMA` (ignoring column order). |
| `top_k` | `int` | `100` | Number of most-frequent text values to keep. |

**Returns:** `None` (mutates `self.common_text_values` in place).

**Raises:** `ValueError` if `events` isn't a `DataFrame` or doesn't match `MEDS_EVENT_SCHEMA`; re-raises (after resetting `common_text_values` to `None`) on any other failure.

---

### `Ontology.rollup_concepts(events, concept_schema, threshold=0.01, rollup=True)`

Prunes rare, non-measurement codes out of `self.codes`, and (if `rollup=True`) maps each pruned code to its nearest same-domain ancestor that *is* common enough to keep, so that a downstream tokenizer can substitute the ancestor when it sees the rare code.

**Parameters**

| Name | Type | Default | Description |
| --- | --- | --- | --- |
| `events` | `pyspark.sql.DataFrame` | — | Schema `\|patient_id\|code\|time\|end\|numeric_value\|text_value\|decile_value\|unit\|event_type\|visit_id\|`. |
| `concept_schema` | `pyspark.sql.DataFrame` | — | MEDS `ConceptSchema` DataFrame, needed for its `ancestors` column. |
| `threshold` | `float` | `0.01` | Minimum fraction of distinct patients a (non-measurement) code must occur in to be kept as-is. |
| `rollup` | `bool` | `True` | If `False`, only the pruning step runs (`rollup_map` stays `{}`); the ancestor-mapping step is skipped entirely. |

**Returns:** `None` (mutates `self.codes` and `self.rollup_map` in place).

**⚠ Side effect:** this method mutates `self.codes` directly, in addition to computing `rollup_map` — a code that falls below `threshold` is removed from `self.codes` regardless of whether `rollup=True` or `False`. `concepts.OMOP_BIRTH` and `concepts.OMOP_DEATH` are always exempted from both pruning and rollup.

**Behavior (when `rollup=True`)**

1. Compute each non-measurement code's prevalence (fraction of distinct patients with that code).
2. Drop codes below `threshold` from `self.codes` (except birth/death).
3. For each dropped code, from its same-domain ancestors that are *above* threshold, pick the nearest one (`min_levels_of_separation`) as its rollup target.
4. Remove birth/death from `rollup_map` even if they'd otherwise qualify.

**Raises:** re-raises after resetting `rollup_map` to `None`.

---

### `Ontology.load_from_disk(ontology_data_dir, overwrite=True)`

Loads a previously-saved ontology from a directory of `.json` files (one per field).

**Parameters**

| Name | Type | Default | Description |
| --- | --- | --- | --- |
| `ontology_data_dir` | `str` | — | Directory containing `codes.json`, `factors.json`, `event_types.json`, `qualifiers.json`, `bins.json`, `units.json`, `common_text_values.json`, `code_to_event_type.json`, `code_to_name.json`, `code_to_qualifiers.json`, `code_to_factors.json`, `code_to_unit.json`, `code_to_bin_ranges.json`, `rollup_map.json`. |
| `overwrite` | `bool` | `True` | If `False`, raises if any field is already loaded (non-`None`) on `self`. |

**Returns:** `None` (mutates `self` in place). Integer-keyed maps (including the nested `code`/`decile` keys of `code_to_bin_ranges`) are converted back from JSON's string keys to `int`; unit keys stay strings.

**Raises:** `Exception` if `ontology_data_dir` doesn't exist, if `overwrite=False` and a field is already loaded, or if any expected `.json` file is missing. Re-raises (after resetting every field to `None`) on any read failure.

---

### `Ontology.save_to_disk(ontology_data_dir, overwrite=True)`

Writes every field to its own `.json` file in `ontology_data_dir` (the inverse of `load_from_disk`).

**Parameters**

| Name | Type | Default | Description |
| --- | --- | --- | --- |
| `ontology_data_dir` | `str` | — | Must already exist. |
| `overwrite` | `bool` | `True` | If `False`, raises if any target `.json` file already exists. |

**Returns:** `None`.

**Raises:** `Exception` if `ontology_data_dir` doesn't exist, if any field on `self` is still `None` (i.e. hasn't been computed yet), or if `overwrite=False` and a target file already exists.

---

## Example

```python
from meds_biobank.ontologies.ontologies import Ontology

# build from scratch
ontology = Ontology()
ontology.compute_concept_ontology(events, concept_schema)
ontology.bin_measurements(events)
ontology.bin_text_values(events)
ontology.rollup_concepts(events, concept_schema)
ontology.save_to_disk(str(ontology_data_dir), overwrite=True)

# reload later
new_ontology = Ontology()
new_ontology.load_from_disk(str(ontology_data_dir), overwrite=False)
```
