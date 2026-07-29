class Tokenizer():
    def __init__(self, ontology, ontology_rollout=False, domain_rollout=False, qualifier_rollout=False):
        self.ontology = ontology # ontology object
        self.ontology_rollout = ontology_rollout # whether to use ontology rollout
        self.domain_rollout = domain_rollout # wither to use domain rollout
        self.qualifier_rollout = qualifier_rollout # whether to use qualifier rollout
        self.symbol_to_token = None
        self.token_to_symbol = None

    def build_vocab(self):
        """
        Use ontology to create vocab (codes, domains, qualifiers, deciles)
        """
        symbol_to_token = {}
        idx = 0
        for symbol in self.ontology.codes:
            symbol_to_token[symbol] = idx
            idx += 1
        for symbol in self.ontology.domains:
            symbol_to_token[symbol] = idx
            idx += 1
        for symbol in self.ontology.qualifiers:
            symbol_to_token[symbol] = idx
            idx += 1
        for symbol in self.ontology.deciles:
            symbol_to_token[symbol] = idx
            idx += 1
        self.symbol_to_token = symbol_to_token
        self.token_to_symbol = {v:k for k,v in symbol_to_token.items()}

    def tokenize(self, events):
        """
        Args:
            events (List<Dict>): |patient_id|code|time|end|numeric_value|text_value|unit|event_type|visit_id| (single patient)
        Returns:
            tokens (List<int>), times (List<timestamp>), visits (List<int>)
        """

        # catch error: vocab not built yet
        if self.symbol_to_token is None:
            raise Exception(f"ERROR: tokenizer.symbol_to_token is None. Have you run tokenizer.build_vocab() yet?")

        # init structures
        tokens = []
        times = []
        visits = []
        last_visit_id = -1
        visit_idx = -1

        # for each concept, lookup token, extract time, and extract visit id
        for event in events:

            # add code token if we have a vocab slot for it: if the concept is not in vocab, lookup in rollup, then if not present here, skip
            code = event["code"]
            if code not in self.ontology.code_to_domain:
                if code in self.ontology.rollup_map:
                    code = self.ontology.rollup_map[code]
                else:
                    continue
            tokens.append(self.symbol_to_token[code])

            # handle time
            time = event["time"]
            times.append(time)

            # handle visit id
            visit_id = event["visit_id"]
            if visit_id == None or visit_id != last_visit_id:
                visit_idx += 1
                visits.append(visit_idx)
            else:
                visits.append(visit_idx)

            # for a concept with a labs_ or vitals_ domain, bin its numeric value if present and tokenize
            code_domain = self.ontology.code_to_domain[code]
            if code_domain.startswith("labs_") or code_domain.starts_with("vitals_"):
                try:
                    value = float(code["numeric_value"])
                except:
                    continue
                decile_ranges = self.ontology.domain_to_decile_ranges[code_domain]
                for bucket in decile_ranges:
                    if bucket["min"] <= value and value <= bucket["max"]:
                        decile = bucket
                        value_token = self.symbol_to_token[f"decile{decile}"]
                        tokens.append(value_token)
                        times.append(time)
                        visits.append(visit_idx)

        return tokens, times, visits

    def detokenize(self, tokens):
        """
        Args:
            tokens (List<int>), times (List<timestamp>), visits (List<int>)
        Returns:
            events (List<Dict>): |patient_id|code|time|end|numeric_value|text_value|unit|event_type|visit_id| (single patient)
        """
        pass

class MetaTokenizer():
    def __init__(self, tokenizer):
        self.tokenizer = tokenizer
    def build_vocab(self):
        pass
    def tokenize(self, events):
        """
        Args:
            tokens (List<int>), times (List<timestamp>), visits (List<int>)
        Returns:
            tokens (List<int>)
        """
        pass
    def detokenize(self, tokens):
        """
        Args:
            tokens (List<int>)
        Returns:
            tokens (List<int>), times (List<timestamp>), visits (List<int>)
        """
        pass