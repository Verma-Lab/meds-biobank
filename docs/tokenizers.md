# Tokenizers

### `BaseTokenizer(ontology, qualifiers=False, event_types=False, factors=False, factor_types=[])`

- Base Tokenizer for MEDS events. Does not handle advanced sequence representations such as time tokens, token rollout, or textualization.
- Build tokenizer vocab based on ontology symbols. Tokenize MEDS event streams to return aligned sequences of code, qualifier, event_type, and factor tokens, and datetime time stamps.
- Assign deciles for measurement values based on ontology and inject into token stream after measurement concept tokens. Rollout qualifier, event_type, factor fields as None. Propagate msmt timestamp.
- Inject BOS at beginning with None fields for all other fields. Inject BOV at beginning of new visit with time equal to time of first event in visit.
- Detokenize token streams of the same format as tokenizer output.

* **Parameters:**
  * `ontology` (meds_biobank.ontologies.ontologies.Ontology): ...
  * `qualifiers` (bool, default=False): whether to allocate and return tokens for BaseTokenizer.ontology.qualifiers of each code (lookup via BaseTokenizer.ontology.code_to_qualifiers)
  * `event_types` (bool, default=False): whether to allocate and return tokens for BaseTokenizer.ontology.event_types of each code (lookup via BaseTokenizer.ontology.code_to_event_types)
  * `factors` (bool, default=False): whether to allocate and return tokens for BaseTokenizer.ontology.factors of each code (lookup via BaseTokenizer.ontology.code_to_factors).
  * `factor_types` (List[str], default=[]): list of event_types to return factors for, assuming BaseTokenizer.factors is set to True. Must be valid BaseTokenizer.ontology.event_types.

* **Fields:**
  * `ontology`
  * `qualifiers`
  * `event_types`
  * `factors`
  * `factor_types`
  * `symbols` (List[object]): list of all tokenizer symbols. Includes codes, bins, and special tokens BOS/BOV. Includes quals, event_types, and factors based on user settings.
  * `symbol_to_id` (Dict[object, int]): tokenizer table of symbol to tokenizer id
  * `id_to_symbol` (Dict[int, object]): inverse of symbol_to_id

* **Methods:**
  * `build_vocab(self)`
    * Construct BaseTokenizer.symbols, BaseTokenizer.symbol_to_id, and BaseTokenizer.id_to_symbol for all desired symbols in ontology based on BaseTokenizer's boolean Parameters.
  * `tokenize(self, events)`
    * Tokenize a partient MEDS event stream using the fields populated by build_vocab and returning the fields requested via BaseTokenizer's boolean Parameters.

* **Example:**
```python
# read meds events
REPO_ROOT = Path(__file__).resolve().parents[2]
MEDS_DATA_DIR = REPO_ROOT / os.environ["MEDS_DATA_DIR"]
meds_events_path = MEDS_DATA_DIR / "meds_events.parquet"
events = spark.read.parquet(str(meds_events_path))

# read ontology
ontology_data_dir = REPO_ROOT / os.environ["ONTOLOGY_DATA_DIR"]
ontology = Ontology()
ontology.load_from_disk(str(ontology_data_dir), overwrite=False)

# create base tokenizer
bt = BaseTokenizer(
    ontology,
    qualifiers=False,
    event_types=True,
    factors=True,
    factor_types=["drug", "procedure", "measurement", "condition", "observation"]
)
bt.build_vocab()  

# load a patient
pt_ids = [row["patient_id"] for row in meds_events.select("patient_id").distinct().collect()]
rand_id = random.choice(pt_ids)
pt_rows = meds_events.filter(F.col("patient_id") == rand_id).orderBy("time") # ensure ordering
events = [
    {
        "patient_id": row["patient_id"],
        "code": row["code"],
        "time": row["time"],
        "end": row["end"],
        "numeric_value": row["numeric_value"],
        "text_value": row["text_value"],
        "unit": row["unit"],
        "event_type": row["event_type"],
        "visit_id": row["visit_id"]
    } for row in pt_rows.collect()
]  

# tokenize the patient
tokens = bt.tokenize(events)  

# detokenize the patient
decoded_events = bt.detokenize(tokens)
```