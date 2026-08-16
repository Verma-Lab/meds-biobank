from meds_biobank.ontologies.ontologies import Ontology
import math

SPECIAL_TOKENS = ["BOS", "BOV"]

class BaseTokenizer():

    def __init__(
        self,
        ontology,
        qualifiers=False,
        event_types=False,
        factors=False,
        factor_types=[] # any list of valid event_types
    ):

        # error guards
        if not isinstance(ontology, Ontology):
            raise ValueError(f"Tokenizer.__init__: ontology must be of type Ontology but is of type {type(ontology)}.")
        if factors and (len(factor_types) == 0):
            raise ValueError(f"Tokenizer.__init__: factors selected for return but no factor types specified.")
        if not factors and (len(factor_types) != 0):
            raise ValueError(f"Tokenizer.__init__: factors not selected for return but factors types {factor_types} requested specifically.")
        for ft in factor_types:
            if ft not in ontology.event_types:
                raise ValueError(f"Tokenizer.__init__: factors requested for event_type {ft} but ontology only has {ontology.event_types}.")
        
        # init input fields
        self.ontology = ontology
        self.qualifiers = qualifiers
        self.event_types = event_types
        self.factors = factors
        self.factor_types = factor_types

        # register tokenizer fields
        self.symbols = None
        self.symbol_to_id = None
        self.id_to_symbol = None
        self.next_id = 0

    def build_vocab(self):

        try:

            # init tokenizer fields
            self.symbols = set()
            self.symbol_to_id = {}
            self.id_to_symbol = {}

            # add codes
            for code in self.ontology.codes:
                self.symbol_to_id[code] = self.next_id
                self.next_id += 1
            
            # add bins
            for bin in self.ontology.bins:
                self.symbol_to_id[bin] = self.next_id
                self.next_id += 1

            # add special tokens
            for st in SPECIAL_TOKENS:
                self.symbol_to_id[st] = self.next_id
                next_id += 1

            # add qualifiers if requested
            if self.qualifiers:
                for qual in self.ontology.qualifiers:
                    self.symbol_to_id[qual] = self.next_id
                    self.next_id += 1
                
            # add event_types if requested
            if self.event_types:
                for et in self.event_types:
                    self.symbol_to_id[et] = self.next_id
                    self.next_id += 1
            
            # add factors if requested
            if self.factors:
                for fact in self.factors:
                    if fact not in self.symbol_to_id:
                        self.symbol_to_id[fact] = self.next_id
                        self.next_id += 1

            # build id_to_symbol from symbol_to_id
            id_to_symbol = {v:k, for k,v in self.symbol_to_id}
        
        except:

            # de-init tokenizer fields in case of failure (then raise error)
            self.symbols = None
            self.symbol_to_id = None
            self.id_to_symbol = None
            raise

    def tokenize(self, events):
        """
        Args:
            events (List<Dict>): |patient_id|code|time|end|numeric_value|text_value|unit|event_type|visit_id| (single patient)
        Returns:
            ...
        """

        # init token list, add BOS
        tokens = {
            "codes": [self.symbol_to_id["BOS"]],
            "times": [None]
        }
        if self.qualifiers:
            tokens["qualifiers"] = [None]
        if self.event_types:
            tokens["event_types"] = [None]
        if self.factors:
            tokens["factors"] = [None]
        
        # init tracking var
        last_visit_id = -1

        # iterate through events
        for event in events:

            # if new visit, add BOV token
            if event["visit_id"] is not None:
                if event["visit_id"] != last_visit_id:

                    # add tokens
                    tokens["codes"].append(self.symbol_to_id["BOV"])
                    tokens["times"].append(event["time"])
                
                    # rollout fields
                    if self.qualifiers:
                        tokens["qualifiers"].append(None)
                    if self.event_types:
                        tokens["event_types"].append(None)
                    if self.factors:
                        tokens["factors"].append(None)
                    last_visit_id = event["visit_id"]

            # extract primary code
            code = event["code"]

            # if code is not in the ontology, special logic
            if code not in self.ontology.codes:

                # if code can be rolled up, roll it up and then look up token as normal in symbol table
                if code in self.ontology.rollup_map:
                
                    # convert the code
                    code = self.ontology.rollup_map[code]

                    # try to get the token for the code
                    code_token = self.symbol_to_id[code]
                
                # if code cannot be rolled up, give up and set its token to none
                else:
                    code_token = None

            # otherwise if we know the code, look up token as normal in symbol table
            else:
                code_token = self.symbol_to_id["code"]
            
            # add token to growing list
            tokens["codes"].append(code_token)

            # if code is msmt code, handle value
            if code in self.ontology.code_to_bin_ranges:
                value = event["numeric_value"]
                if value != None:
                    value = float(value)
                    if value <= 0.0:
                        value_code = f"bin_0"
                    else:
                        value = math.log1p(value)
                        for bin, range in self.ontology.code_to_bin_ranges[code].items():
                            mini = float(range["min"])
                            maxi = float(range["max"])
                            if (mini <= value) and (value < maxi):
                                break
                        value_code = f"bin_{bin}"
                    value_token = self.symbol_to_id[value_code]
                else:
                    value_token = None
            else:
                value_token = None
            tokens["codes"].append(value_token)

            # handle times
            tokens["times"].append(event["time"])
            if value_token is not None:
                tokens["times"].append(None)

            # handle factors if requested
            if self.factors and event["event_type"] is in self.factor_types:
                factor_codes = self.ontology.code_to_factors[code]
                factor_tokens = []
                for fc in factor_codes:
                    factor_tokens.append(self.symbol_to_id[fc])
                tokens["factors"].append(factor_tokens)
                if value_token is not None:
                    tokens["factors"].append(None)

            # handle qualifiers if requested
            if self.qualifiers:
                qualifiers = self.ontology.code_to_qualifiers[code]
                qualifier_tokens = []
                for qual in qualifiers:
                    qualifier_tokens.append(self.symbol_to_id[qual])
                tokens["qualifiers"].append(qualifier_tokens)
                if value_token is not None:
                    tokens["qualifiers"].append(None)
            
            # handle event types
            if self.event_types:
                event_type_code = event["event_type"]
                event_type_token = self.symbol_to_id[event_type_token]
                tokens["event_types"].append(event_type_token)
                if value_token is not None:
                    tokens["event_types"].append(None)
            
            return tokens
            

    def detokenize(self, tokens):
        """
        Args:
            tokens (Dict<List<int>>)
        Returns:
            events (List<Dict>): |patient_id|code|time|end|numeric_value|text_value|unit|event_type|visit_id| (single patient)
        """

        # init events
        events = []

        # register length of codes (tokens)
        n = len(tokens["codes"])

        # begin counter
        i = 0

        # init visit id
        visit_id = 0

        # iterate through code tokens
        while i < n:
            
            # extract predicted code
            code_token = tokens["codes"][i]
            code = self.id_to_symbol[code_token]

            # handle beginning of visit
            if code == "BOV":
                visit_id += 1
                i += 1
                continue
            
            # skip beginning of sequence
            if code == "BOS":
                continue

            # init blank event
            event = {
                "patient_id": None,
                "code": None,
                "time": None,
                "end": None,
                "numeric_value": None,
                "text_value": None,
                "unit": None,
                "event_type": None,
                "visit_id": None
            }

            # set event code
            event["code"] = code

            # set event time
            event["time"] = tokens["times"][i]

            # set event event_type
            event[event_type] = self.ontology.code_to_event_type[code]

            # set event numeric_value and unit
            if i <= (n-2):

                # if next token is a decile bin
                next_token = tokens["codes"][i+1]
                next_token_code = self.ontology.id_to_symbol[next_token]
                if next_token_code.startswith("bin_"):

                    # if we can translate bins for this code
                    if code in self.ontology.code_to_bin_ranges:

                        # impute value, unit, increment i by (an extra) 1 (so it increments by two by end)
                        bin_int = int(next_token_code.split(".")[1]
                        mini = float(self.ontology.code_to_bin_ranges[code][bin_int]["min"])
                        maxi = float(self.ontology.code_to_bin_ranges[code][bin_int]["max"])
                        event["numeric_value"] = (mini+maxi)/2
                        event["unit"] = self.ontology.code_to_unit[code]
                        i += 2
                        continue

            # increment counter for manual loop
            i += 1

class TimeTokenizer():
    def __init__(base_tokenizer, method="approximate"):

        # guard against type errors
        if not isinstance(base_tokenizer, BaseTokenizer):
            raise ValueError("TimeTokenizer.__init__(): base_tokenizer is not of class BaseTokenizer.")

        # guard against invalid method arguments
        if method is not in {"approximate", "exact"}:
            raise ValueError(f"TimeTokenizer.__init__(): method is supposed to be either \"approximate\" or \"exact\" but detected {method}.")

        # ensure that base tokenizer has already set vocab
        if None in (base_tokenizer.symbols, base_tokenizer.symbol_to_id, base_tokenizer.id_to_symbol):
            raise Exception("TimeTokenizer.__init__(): base_tokenizer does not have its symbol vocabulary built fully (or at all).")

        # init fields
        self.base_tokenizer = base_tokenizer
        self.method = method

    def augment_vocab(self):

        # add exact time bins if exact time passage method chosen
        if self.method == "exact":
            for i in range(60):
                self.ontology.symbol_to_id[f"minutes_{i}"] = self.ontology.next_id
                self.ontology.next_id += 1
            for i in range(24):
                self.ontology.symbol_to_id[f"hours_{i}"] = self.ontology.next_id
                self.ontology.next_id += 1
            for i in range(7):
                self.ontology.symbol_to_id[f"days_{i}"] = self.ontology.next_id
                self.nontology.ext_id += 1
            for i in range(52):
                self.ontology.symbol_to_id[f"weeks_{i}"] = self.ontology.next_id
                self.ontology.next_id += 1
            for i in range(100):
                self.ontology.symbol_to_id[f"years_{i}"] = self.ontology.next_id
                self.ontology.next_id += 1
        
        # add approximate time bins if approximate time passage method chosen
        elif self.time_passage == "approximate":
            time_symbols = ["5m-15m", "15m-1h", "1h-2h", "2h-6h", "6h-12h", "12h-1d", "1d-3d", "3d-1w", "1w-2w", "2w-1mt", "1mt-3mt", "3mt-6mt", "=6mt"]
            for tsymb in time_symbols:
                self.ontology.symbol_to_ids[tsymb] = self.ontology.next_id
                self.ontology.next_id += 1
    
    def tokenize(self, events):

        # compute base tokens
        temp_tokens = self.base_tokenizer.tokenize()

        # TODO: inject time tokens into code tokens based on chosen method
        tokens = ...

        return tokens

    def detokenize(self, tokens):
        
        # TODO: use injected time tokens to assign times to basic events
        temp_tokens = ...

        return self.base_tokenizer.detokenize(temp_tokens)
    
    def tokenize_time_diff(time_diff, method="exact"):

        # TODO: tokenize time difference given method (e.g. 1 hr 1 min, exact -> {1 hr}{1 min})

class RolloutTokenizer():
    pass

class Textualizer():
    pass