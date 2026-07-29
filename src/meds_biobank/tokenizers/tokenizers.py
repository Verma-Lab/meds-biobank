class Tokenizer():
    def __init__(self, ontology):
        self.ontology = ontology # concept_name, concept_id/code
        self.symbols = {
            "codes": [], # will include all omop concepts
            "bins": [], # will include bins 0-10
            "tags": [], # will include special tags like BOS and EOS
            "custom": [], # will include custom concept codes
            "time": [], # will include all time passage codes
            "classifications": [] # will include all classification codes
        }
        self.symbol_to_token = None # will map all symbols to integer token ids
        self.token_to_symbol = None # will map all integer token ids to symbols
        self.symbol_to_name = None # will map all token ids to concept names
        self.code_to_classifications = None # will map all token ids to classifications
        self.code_to_domain = None # will map all omop concepts to domain/event_type

    def build_vocab(self, concepts, ccs=False, phecodes=False, med_ingred=False):
        """
        Args:
            concepts (pyspark.sql.DataFrame):
                Desc: OMOP concepts table
                Schema: |concept_id|vocabulary_id|concept_code|...
            ccs (bool):
                Desc: use ccs to build token_to_classifications
            phecodes (bool):
                Desc: use phecodes to build token_to_classifications
            med_ingred (bool):
                Desc: use med ingredients to build token_to_classifications
        
        Creates:
            mappings (see init)
        """
        # build code_to_domain
        self.code_to_domain = {
            row["concept_id"]: row["vocabulary_id"]
            for row in concepts.select("concept_id", "vocabulary_id").collect()
        }

        # TODO: build code_to_classifications
        self.code_to_classifications = None

        # create symbol table
        omop_codes = [row["concept_id"] for row in concepts.select("concept_id").distinct().collect()]
        self.symbols["codes"] = omop_codes
        self.symbols["bins"] = [str(i) for i in range(11)]
        self.symbols["tags"] = ["BOS", "EOS"]
        self.symbols["custom"] = list(self.custom_concepts.values())
        self.symbols["time"] = ["{i}d" for i in range(1,7)] + ["{i}mo" for i in range(1, 12)] + ["{i}yr" for i in range(1,100)]
        self.symbols["classifications"] = [] # TODO
    
    def load_vocab(self, load_dir):
        """
         Args:
            load_dir (String):
                Desc: path to save dir
        
        Loads:
            mappings (see init)
        """
        # TODO: load vocab mappings
        pass
    
    def save_vocab(self, save_dir):
        """
         Args:
            save_dir (String):
                Desc: path to save dir
        
        Saves:
            mappings (see init)
        """
        # TODO: save vocab mappings
        pass
    
    def tokenize(self, events, rollout=False):
        """
        Args:
            events (List<Dict>):
                Desc: Records for a single patient, ordered by time, asc (birth event is first!)
                Dict Schema: |patient_id|code|time|end|numeric_value|text_value|unit|event_type|visit_id|
                Notes:
                    • code = concept_id
                    • text_value for labs_/vitals_ event types will be bins and be tokenized separately after the concept itself
                    • raw measurement events will have value skipped
                    • add BOS
            rollout (bool):
                Desc: if true, apply rollout tokenization using classifications preceding specific code token(s)
        Returns:
            tokens (List<int>), times (List<timestamp>)
        """
        if self.symbol_to_token is None:
            raise Exception(f"ERROR: Vocabulary has not been built/loaded yet!")
        tokens = []
        times = []
        for event in events:
            new_tokens, new_times = self._tokenize_event(event, rollout=rollout)
            tokens += new_tokens
            times += new_times
        # TODO: add BOS
        # TODO: add EOS
        return tokens, times
    
    def _tokenize_event(self, event, rollout=False):
        """
        Args:
            event (Dict):
                Desc: Records for a single patient event
                Dict Schema: |patient_id|code|time|end|numeric_value|text_value|unit|event_type|visit_id|
                Notes:
                    • code = concept_id
                    • text_value for labs_/vitals_ event types will be bins and be tokenized separately after the concept itself
                    • raw measurement events will have value skipped
                    • add BOS
            rollout (bool):
                Desc: if true, apply rollout tokenization using classifications preceding specific code token(s)
        Returns:
            tokens (List<int>):
                Desc: list of token ids
            times (List<timestamp>)
                Desc: List of timestamps associated with tokens (len(times) = len(tokens))
        """

        # tokenize code
        code = event["code"]
        code_token = self.symbol_to_token[code]

        # tokenize value if present
        value = event["text_value"]
        value_token = None
        if value != None and value in self.symbol_to_token:
            value_token = self.symbol_to_token[value]
        
        # TODO: perform rollout tokenization if requested
        rollout_tokens = []

        # synthesize tokens
        tokens = [code_tokens]
        tokens += rollout_tokens
        if value_token is not None:
            tokens.append(value_token)
        
        # synthesize times
        event_time = event["time"]
        times = [event_time * len(tokens)]

        return tokens, times
    
    def insert_time_tokens(self, tokens, times):
        """
        Args:
            tokens (List<int>):
                Desc: list of token ids
            times (List<timestamp>)
                Desc: List of timestamps associated with tokens (len(times) = len(tokens))
        Returns:
            tokens (List<int>):
                Desc: list of token ids w/ time tokens inserted
            mask (List<bool>):
                Desc: length of tokens, 0/1 for isTimeToken/~isTimeToken (use later for demographics-aware truncation in dataloader)
        """
        pass
    
    def _translate_tokens(self, tokens):
        """
        Args:
            tokens (List<int>):
                Desc: list of token ids
        Returns:
            tokens (List<int>):
                Desc: list of symbols translated directly from tokens
        Notes:
            • Skip BOS, EOS
        """
        pass
    
    def decode_into_events(self, tokens, dob=None, rollout=False):
        """
        Args:
            tokens (List<int>):
                Desc: list of token ids
            dob (datetime):
                Desc: dob datetime
            rollout (bool):
                Desc: whether to be sensitive to rollout and absorb into single event (classifications tokens)
        Returns:
            events (List<Dict>):
                Desc:
                Schema: |code|time|text_value|
        Notes:
            • Assign first event time 00:00:00 unless dob datetime specified
        """
        pass
    
    def decode_into_events_verbose(self, tokens, lv_code_mapping, lv_unit_mapping, decile_mapping, dob=None, rollout=False):
        """
        Args:
            tokens (List<int>):
                Desc: list of token ids
            dob (datetime):
                Desc: dob datetime
            rollout (bool):
                Desc: whether to be sensitive to rollout and absorb into single event (classifications tokens)
            lv_code_mapping ():
                Desc: Maps msmt codes to labs/vitals types
            lv_unit_mapping ():
                Desc: labs/vitals types to standardized units
            decile_mapping ():
                Desc: Maps labs/vitals types to decile ranges
        Returns:
            events (List<Dict>):
                Desc:
                Schema: |patient_id|code|time|end|numeric_value|text_value|unit|event_type|visit_id|
        Notes:
            • Assign random patient_id, visit_id
            • Assign first time 00:00:00 unless dob datetime specified and first event is birth
            • Assign end=None, text_value=None
            • Assign numeric values from decile range for lab/vital type of msmt concept id if it maps
            • Assign unit from lab/vital type of msmt concept id if it maps
        """
        pass