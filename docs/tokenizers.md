# meds_biobank.tokenizers

Turn per-patient MEDS event streams into integer token sequences (and back). Three tokenizers layer on top of each other:

| Class | Wraps | Adds |
| --- | --- | --- |
| [`BaseTokenizer`](#basetokenizerontology-qualifiersfalse-event_typesfalse-factorsfalse-unitstrue-factor_types) | — | code / value / unit tokens, aligned parallel metadata lists |
| [`TimeTokenizer`](#timetokenizerbase_tokenizer-methodapproximate) | a `BaseTokenizer` | interleaved time-passage tokens between events |
| [`RolloutTokenizer`](#rollouttokenizertokenizer-qualifiersfalse-event_typesfalse-factorstrue) | a `BaseTokenizer` or `TimeTokenizer` | flattens the parallel metadata lists into the code sequence itself, delimited by `BOE` |

**Source:** `src/meds_biobank/tokenizers/tokenizers.py`

---

## `BaseTokenizer(ontology, qualifiers=False, event_types=False, factors=False, units=True, factor_types=[])`

Base tokenizer for MEDS event streams. Builds a flat vocabulary from an [`Ontology`](./ontologies.md) and converts patient event lists to/from integer token sequences. Does not know about elapsed time or fully-flat rollout — see `TimeTokenizer` and `RolloutTokenizer` for those.

A code that isn't in `ontology.codes` is rolled up to its nearest ancestor via `ontology.rollup_map` if one exists; otherwise the event is silently dropped from the output (no token is emitted for it).

### Parameters

| Name | Type | Default | Description |
| --- | --- | --- | --- |
| `ontology` | [`Ontology`](./ontologies.md) | — | Must have its vocabulary already computed (`compute_concept_ontology` + `bin_measurements` + `bin_text_values` + `rollup_concepts`, or `load_from_disk`). |
| `qualifiers` | `bool` | `False` | Whether to allocate vocab for, and return, per-event qualifier tokens (`ontology.qualifiers`, looked up per-code via `ontology.code_to_qualifiers`). |
| `event_types` | `bool` | `False` | Whether to allocate vocab for, and return, a per-event event-type token (`ontology.event_types`, looked up per-code via `ontology.code_to_event_type`). |
| `factors` | `bool` | `False` | Whether to allocate vocab for, and return, per-event factor tokens (`ontology.factors`, looked up per-code via `ontology.code_to_factors`). Requires `factor_types` to be non-empty. |
| `units` | `bool` | `True` | Whether to allocate vocab for `ontology.units`. Measurement value tokenization (below) still needs this to be `True` to emit unit tokens. |
| `factor_types` | `List[str]` | `[]` | Event types to actually return factors for when `factors=True` (e.g. `["drug", "condition"]`). Each entry must be a member of `ontology.event_types`. Must be empty when `factors=False`. |

### Raises

- `ValueError` — `ontology` is not an `Ontology` instance.
- `ValueError` — `factors=True` but `factor_types` is empty, or `factors=False` but `factor_types` is non-empty.
- `ValueError` — an entry of `factor_types` is not in `ontology.event_types`.

### Attributes

| Name | Type | Description |
| --- | --- | --- |
| `ontology`, `qualifiers`, `event_types`, `factors`, `factor_types`, `units` | — | Copies of the constructor arguments. |
| `symbols` | `Set[object]` \| `None` | All vocabulary symbols. `None` until `build_vocab()` runs. |
| `symbol_to_id` | `Dict[object, int]` \| `None` | Symbol → token id. `None` until `build_vocab()` runs. |
| `id_to_symbol` | `Dict[int, object]` \| `None` | Inverse of `symbol_to_id`. `None` until `build_vocab()` runs. |
| `next_id` | `int` | Next free token id; `0` until `build_vocab()` runs. |

---

### `BaseTokenizer.build_vocab()`

Populates `symbols`, `symbol_to_id`, and `id_to_symbol` from the ontology, in this fixed order: `ontology.codes` → `ontology.bins` (decile bins) → `ontology.common_text_values` → special tokens `BOS`, `BOE` → `ontology.qualifiers` (if `qualifiers=True`) → `ontology.event_types` (if `event_types=True`) → `ontology.factors` (if `factors=True`, skipping anything already assigned) → `ontology.units` (if `units=True`). Must be called before `tokenize()` or `detokenize()`.

On any failure, resets `symbols`/`symbol_to_id`/`id_to_symbol` to `None` and `next_id` to `0`, then re-raises.

**Parameters:** none. **Returns:** `None` (mutates `self` in place).

---

### `BaseTokenizer.tokenize(events)`

Converts one patient's event list into aligned token sequences.

**Parameters**

| Name | Type | Description |
| --- | --- | --- |
| `events` | `List[Dict]` | One patient's events, each a dict with keys `patient_id`, `code`, `time`, `end`, `numeric_value`, `text_value`, `unit`, `event_type`, `visit_id` (see the [MEDS DataSchema](./etl_pipelines.md#schemas)). |

**Returns:** `tokens` (`Dict`)

| Key | Type | Present when | Description |
| --- | --- | --- | --- |
| `codes` | `List[int]` | always | Token ids, `BOS`-prefixed. |
| `times` | `List[datetime \| None]` | always | Aligned to `codes`; `None` for `BOS`. |
| `visit_ids` | `List[int \| None]` | always | Aligned to `codes`; a dense re-index of each event's `visit_id` starting at `0`, scoped to this call (not the original `visit_id` value, and not stable across calls). `None` for events with no `visit_id`. |
| `qualifiers` | `List[List[int] \| None]` | `self.qualifiers` | Aligned to `codes`. `None` for `BOS` and for value/unit tokens; an (possibly empty) list for every primary code token. |
| `event_types` | `List[int \| None]` | `self.event_types` | Aligned to `codes`. `None` for `BOS` and value/unit tokens. |
| `factors` | `List[List[int] \| None]` | `self.factors` | Aligned to `codes`. `None` unless the code's event type is in `self.factor_types` *and* the code has factors in `ontology.code_to_factors`. |

**Behavior notes**

- `BOS` is always the first token, with `None` in every other list.
- For `event_type == "measurement"` codes with decile ranges available for the event's `unit` (`ontology.code_to_bin_ranges[code][unit]`) and a non-null `numeric_value`, a decile-bin token (`bin_0`–`bin_10`, computed via `log1p`) plus a unit token are appended right after the code token, and they share the code's `time`/`visit_id`. `bin_0` is used for `numeric_value <= 0.0`.
- If no bin/unit pair applies, and `text_value` is one of `ontology.common_text_values`, a single text-value token is appended instead.
- If neither applies, only the code token is emitted for that event.

**Raises:** `ValueError` if `build_vocab()` has not been run yet.

---

### `BaseTokenizer.detokenize(tokens)`

Inverse of `tokenize()`. Reconstructs an event list from a token dict of the same shape `tokenize()` returns.

**Parameters**

| Name | Type | Description |
| --- | --- | --- |
| `tokens` | `Dict` | Same schema as `tokenize()`'s return value. |

**Returns:** `events` (`List[Dict]`), each with keys `patient_id`, `code`, `time`, `end`, `numeric_value`, `text_value`, `unit`, `event_type`, `visit_id`.

**Behavior notes / known limitations**

- `patient_id`, `end`, and `visit_id` are **not** reconstructed — they are always `None` in the output, even though `visit_id` is present in the input `tokens`. Only `code`, `time`, `event_type`, `numeric_value`, `text_value`, and `unit` are actually rebuilt.
- `code` is set to the string symbol (not the original integer code) — for example a rolled-up code will decode to its ancestor, not the original code.
- A decile-bin numeric value is imputed as the midpoint of the bin's `[min, max)` range in `log1p` space, then inverted via `expm1` (i.e. it is an approximation of the original value, not the original value itself). `bin_0` always decodes to exactly `0.0`.

**Raises:** none explicitly, but will raise `KeyError` if `tokens` contains ids not present in `id_to_symbol`.

---

## `TimeTokenizer(base_tokenizer, method="approximate")`

Wraps a `BaseTokenizer` and interleaves elapsed-time tokens between consecutive events. Mutates the wrapped tokenizer's vocabulary (`augment_vocab()` adds directly to `base_tokenizer.symbols` / `symbol_to_id` / `id_to_symbol`) rather than keeping a separate vocabulary.

### Parameters

| Name | Type | Default | Description |
| --- | --- | --- | --- |
| `base_tokenizer` | `BaseTokenizer` | — | Must already have `build_vocab()` run. |
| `method` | `str` | `"approximate"` | `"exact"` or `"approximate"` (see `tokenize_time_diff` below). |

### Raises

- `ValueError` — `base_tokenizer` is not a `BaseTokenizer`.
- `ValueError` — `method` is not `"exact"` or `"approximate"`.
- `Exception` — `base_tokenizer`'s vocabulary has not been built yet.

### Attributes

| Name | Type | Description |
| --- | --- | --- |
| `base_tokenizer`, `method` | — | Copies of the constructor arguments. |
| `time_symbols` | `Set[str]` \| `None` | Time-token vocabulary. `None` until `augment_vocab()` runs. |

---

### `TimeTokenizer.augment_vocab()`

Adds time-passage symbols to both `self.time_symbols` and, in place, to `self.base_tokenizer`'s vocabulary.

- `method="exact"`: adds `minutes_0`–`minutes_59`, `hours_0`–`hours_23`, `days_0`–`days_6`, `weeks_0`–`weeks_51`, `years_0`–`years_99` (243 symbols).
- `method="approximate"`: adds 13 fixed range-bucket symbols: `minutes_5-minutes_15`, `minutes_15-hours_1`, `hours_1-hours_2`, `hours_2-hours_6`, `hours_6-hours_12`, `hours_12-days_1`, `days_1-days_3`, `days_3-weeks_1`, `weeks_1-weeks_2`, `weeks_2-months_1`, `months_1-months_3`, `months_3-months_6`, and the open-ended `months_6`.

**Parameters:** none. **Returns:** `None`.

---

### `TimeTokenizer.tokenize(events)`

Calls `self.base_tokenizer.tokenize(events)` internally, then inserts time tokens (via `tokenize_time_diff`) between positions whose event times differ.

**Parameters:** `events` (`List[Dict]`) — same as `BaseTokenizer.tokenize`.

**Returns:** `tokens` (`Dict`) with keys `codes`, `visit_ids`, and (mirroring `self.base_tokenizer`'s settings) `qualifiers` / `event_types` / `factors`. Inserted time-token positions get `None` in every metadata list. **`times` is not present in the output** — elapsed time is now implicit in the inserted time tokens.

---

### `TimeTokenizer.detokenize(tokens)`

Inverse of `tokenize()`. Reconstructs times from the interleaved time tokens (anchored at the naive timestamp `1970-01-01`, since `TimeTokenizer` only ever knows relative elapsed time, not an absolute start), then delegates the rest of event reconstruction to `self.base_tokenizer.detokenize()`.

**Parameters:** `tokens` (`Dict`) — same schema `tokenize()` returns. **Returns:** `events` (`List[Dict]`) — same as `BaseTokenizer.detokenize`, with the same reconstruction caveats (`patient_id`/`end`/`visit_id` are `None`).

---

### `TimeTokenizer.tokenize_time_diff(time_diff, floor="minutes_5", cieling="years_99")`

Converts a single `timedelta` into a list of time tokens. (Note: the keyword is spelled `cieling` in the source.)

| Name | Type | Default | Description |
| --- | --- | --- | --- |
| `time_diff` | `datetime.timedelta` | — | Elapsed time to tokenize. |
| `floor` | `str` | `"minutes_5"` | Time-code below which no token is emitted (returns `[]`). |
| `cieling` | `str` | `"years_99"` | Time-code above which `time_diff` is capped before tokenizing. |

**Returns:** `List[int]` token ids.

- `method="exact"`: decomposes `time_diff` into years/weeks/days/hours/minutes (naive: `365`-day years, `7`-day weeks) and returns one token per unit.
- `method="approximate"`: returns the single bucket symbol whose range contains `time_diff`, if any. If `time_diff` exceeds the largest bounded bucket, returns one `months_6` token per full 6-month block plus one more if the remainder exceeds 3 months.

---

### `TimeTokenizer.detokenize_time_diff(time_tokens)`

Inverse of `tokenize_time_diff` for a contiguous run of time tokens.

| Name | Type | Description |
| --- | --- | --- |
| `time_tokens` | `List[int]` | A run of time-token ids as produced by `tokenize()`. |

**Returns:** `datetime.timedelta`.

- `[]` → `timedelta(0)`.
- `method="exact"`: sum of each token's `code_to_tdelt`.
- `method="approximate"`, single token: `months_6` → its own timedelta; a range token → the midpoint of its two bounds.
- `method="approximate"`, multiple tokens: `len(time_tokens) * 6 months` (each repeated token is a full 6-month block).

---

### `TimeTokenizer.code_to_tdelt(code)`

Parses a single time symbol into a `timedelta`.

| Name | Type | Description |
| --- | --- | --- |
| `code` | `str` | One of `minutes_i`, `hours_i`, `days_i`, `weeks_i`, `months_i`, `years_i`. |

**Returns:** `datetime.timedelta` (`months` ≈ 30.5 days, `years` ≈ 365 days).

**Raises:** `ValueError` if the magnitude isn't an integer or the unit is unrecognized.

---

## `RolloutTokenizer(tokenizer, qualifiers=False, event_types=False, factors=True)`

Wraps a `BaseTokenizer` or `TimeTokenizer` and fully flattens its output: instead of parallel per-event lists for qualifiers/event-types/factors, every token becomes one element of a single `codes` sequence, with each event's group delimited by a leading `BOE` (beginning-of-event) token: `BOE, [event_type], [qualifier...], [factor...], code, [value/unit...]`.

### Parameters

| Name | Type | Default | Description |
| --- | --- | --- | --- |
| `tokenizer` | `BaseTokenizer` \| `TimeTokenizer` | — | The tokenizer to roll out. |
| `qualifiers` | `bool` | `False` | Emit qualifier tokens in each event's group. Requires the underlying tokenizer to have been built with `qualifiers=True`. |
| `event_types` | `bool` | `False` | Emit an event-type token in each event's group. Requires `event_types=True` on the underlying tokenizer. |
| `factors` | `bool` | `True` | Emit factor tokens in each event's group. Requires `factors=True` on the underlying tokenizer. |

### Raises

- `ValueError` — `tokenizer` is neither a `BaseTokenizer` nor a `TimeTokenizer`.
- `ValueError` — none of `qualifiers`, `event_types`, `factors` is `True`.
- `ValueError` — the wrapped tokenizer's (or its `base_tokenizer`'s) vocabulary isn't built yet.
- `ValueError` — a requested field (`qualifiers`/`event_types`/`factors`) wasn't enabled on the underlying `BaseTokenizer`.

### Attributes

| Name | Type | Description |
| --- | --- | --- |
| `tokenizer` | `BaseTokenizer` \| `TimeTokenizer` | The wrapped tokenizer, as passed in. |
| `base_tokenizer` | `BaseTokenizer` | Resolved to the underlying `BaseTokenizer` even when `tokenizer` is a `TimeTokenizer`. |
| `qualifiers`, `event_types`, `factors` | `bool` | Copies of the constructor arguments. |

---

### `RolloutTokenizer.tokenize(events)`

**Parameters:** `events` (`List[Dict]`) — same as `BaseTokenizer.tokenize`.

**Returns:** `tokens` (`Dict`)

| Key | Type | Description |
| --- | --- | --- |
| `codes` | `List[int]` | Fully unrolled sequence: `BOS`, then per event `BOE, [event_type], [qualifier(s)], [factor(s)], code, [value token(s)]`, with time tokens interleaved between events when wrapping a `TimeTokenizer`. |
| `visit_ids` | `List[int \| None]` | Aligned to `codes`; every token in an event's group shares that event's `visit_id`. `None` for `BOS`/time tokens. |
| `times` | `List[datetime \| None]` | Only present when wrapping a plain `BaseTokenizer` (times are implicit in time tokens when wrapping a `TimeTokenizer`). |

---

### `RolloutTokenizer.detokenize(tokens)`

Inverse of `tokenize()`.

**Parameters:** `tokens` (`Dict`) — output of `tokenize()`.

**Returns:** `events` (`List[Dict]`) — via `self.tokenizer.detokenize(...)` (i.e. delegated to the wrapped `BaseTokenizer`/`TimeTokenizer`, with its same reconstruction caveats).

**Important:** qualifiers and factors are **not** reconstructed from the discarded tokens in each event's run — they are re-derived directly from `ontology.code_to_qualifiers` / `ontology.code_to_factors` for the run's decoded code. This makes `detokenize()` robust to a model predicting the "wrong" qualifier/factor tokens (they're ignored), but it also means a round-trip through `RolloutTokenizer` will silently normalize any such tokens rather than surface a mismatch.

**Raises:** `ValueError` if the sequence is malformed — a `BOE` is expected but not found, or an event's run has no code token before the next delimiter.

---

## Example

```python
from meds_biobank.ontologies.ontologies import Ontology
from meds_biobank.tokenizers.tokenizers import BaseTokenizer, TimeTokenizer, RolloutTokenizer

# load a pre-built ontology
ontology = Ontology()
ontology.load_from_disk(str(ontology_data_dir), overwrite=False)

# base tokenizer: codes + event types + factors for a subset of event types
bt = BaseTokenizer(
    ontology,
    qualifiers=False,
    event_types=True,
    factors=True,
    factor_types=["drug", "procedure", "measurement", "condition", "observation"]
)
bt.build_vocab()

# tokenize / detokenize one patient
tokens = bt.tokenize(events)
decoded_events = bt.detokenize(tokens)

# wrap with elapsed-time tokens
tt = TimeTokenizer(bt, method="exact")
tt.augment_vocab()
time_tokens = tt.tokenize(events)
time_decoded_events = tt.detokenize(time_tokens)

# fully flatten into one BOE-delimited sequence
rt = RolloutTokenizer(tt, event_types=True, factors=True)
rollout_tokens = rt.tokenize(events)
rollout_decoded_events = rt.detokenize(rollout_tokens)
```
