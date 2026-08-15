from meds_biobank.ontologies.ontologies import Ontology
import math

class BaseTokenizer():

    # TODO: implement body

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

        # init token list
        tokens = {
            "codes": [],
            "times": []
        }
        if self.qualifiers:
            tokens["qualifiers"] = []
        if self.event_types:
            tokens["event_types"] = []
        if self.factors:
            tokens["factors"] = []

        # iterate through events
        for event in events:

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
            if self.value_token is not None:
                tokens["times"].append(None)


            # TODO: handle factors if requested
            if self.factors:
                pass

            # TODO: handle qualifiers if requested
            if self.qualifiers:
                pass
                
            

    def detokenize(self, tokens):
        """
        Args:
            ...
        Returns:
            events (List<Dict>): |patient_id|code|time|end|numeric_value|text_value|unit|event_type|visit_id| (single patient)
        """
        pass

class TimeTokenizer():
    def __init__(base_tokenizer, method="approximate"):
        self.base_tokenizer = base_tokenizer
        self.method = method

    def augment_vocab(self):

        # TODO: edit references to be to self.ontology, add tokenize and decode method

        # add exact time bins if exact time passage method chosen
        if self.method == "exact":
            for i in range(60):
                self.symbol_to_id[f"minutes_{i}"] = self.next_id
                self.next_id += 1
            for i in range(24):
                self.symbol_to_id[f"hours_{i}"] = self.next_id
                self.next_id += 1
            for i in range(7):
                self.symbol_to_id[f"days_{i}"] = self.next_id
                self.next_id += 1
            for i in range(52):
                self.symbol_to_id[f"weeks_{i}"] = self.next_id
                self.next_id += 1
            for i in range(100):
                self.symbol_to_id[f"years_{i}"] = self.next_id
                self.next_id += 1
        
        # add approximate time bins if approximate time passage method chosen
        elif self.time_passage == "approximate":
            time_symbols = ["5m-15m", "15m-1h", "1h-2h", "2h-6h", "6h-12h", "12h-1d", "1d-3d", "3d-1w", "1w-2w", "2w-1mt", "1mt-3mt", "3mt-6mt", "=6mt"]
            for tsymb in time_symbols:
                self.symbol_to_ids[tsymb] = self.next_id
                self.next_id += 1

class RolloutTokenizer():
    pass

class Textualizer():
    pass