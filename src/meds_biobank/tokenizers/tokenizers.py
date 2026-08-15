from meds_biobank.ontologies.ontologies import Ontology

class Tokenizer():

    # TODO: implement body

    def __init__(
        self,
        ontology,
        qualifiers="none", # none/return/rollout
        event_types="none", # none/return/rollout
        factors="none", # none/return/rollout
        times="none",  # none/return/rollout
        time_passage="none", # none/approximate/exact
        factor_types=[] # any list of valid event_types
    ):
        """
        Args:
            ontology (object.Ontology):
                Desc: source ontology
            qualifiers (string):
                Desc: tokenizer return method for code qualifiers
            event_types (string):
                Desc: tokenizer return method for code event types
            factors (string):
                Desc: tokenizer return method for code factors
            times (string):
                Desc: tokenizer return method for code event types
            time_passage (string):
                Desc: how to represent the passage of time
            factor_types (List<string>):
                Desc: list of event types for which to return code factors
        """

        # error guards
        if not isinstance(ontology, Ontology):
            raise ValueError(f"Tokenizer.__init__: ontology must be of type Ontology but is of type {type(ontology)}.")
        if qualifiers not in {"none", "return", "rollout"}:
            raise ValueError(f"Tokenizer.__init__: qualifiers must be one of none, return, or rollout but was {qualifiers}.")
        if event_types not in {"none", "return", "rollout"}:
            raise ValueError(f"Tokenizer.__init__: event_types must be one of none, return, or rollout but was {event_types}.")
        if factors not in {"none", "return", "rollout"}:
            raise ValueError(f"Tokenizer.__init__: factors must be one of none, return, or rollout but was {factors}.")
        if times not in {"none", "return", "rollout"}:
            raise ValueError(f"Tokenizer.__init__: times must be one of none, return, or rollout but was {times}.")
        if time_passage not in {"none", "exact", "approximate"}:
            raise ValueError(f"Tokenizer.__init__: time_passage must be one of none, exact, or approximate but was {time_passage}.")
        if (times != "none") and (time_passage == "none"):
            raise ValueError(f"Tokenizer.__init__: times requested for return but no method chosen for time passage.")
        if (times == "none") and (time_passage != "none"):
            raise ValueError("Tokenizer.__init__: no times requested for return but method chosen for time passage.")
        if (factors != "none") and (len(factor_types) == 0):
            raise ValueError(f"Tokenizer.__init__: factors selected for return but no factor types specified.")
        if (factors == "none") and (len(factor_types) != 0):
            raise ValueError(f"Tokenizer.__init__: factors not selected for return but factors types {factor_types} requested specifically.")
        for ft in factor_types:
            if ft not in ontology.event_types:
                raise ValueError(f"Tokenizer.__init__: factors requested for event_type {ft} but ontology only has {ontology.event_types}.")
        
        # init input fields
        self.ontology = ontology
        self.qualifiers = qualifiers
        self.event_types = event_types
        self.factors = factors
        self.times = times
        self.time_passage = time_passage
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

            # add units
            for unit in self.ontology.units:
                self.symbol_to_id[unit] = self.next_id
                self.next_id += 1

            # add qualifiers if requested
            if self.qualifiers != "none":
                for qual in self.ontology.qualifiers:
                    self.symbol_to_id[qual] = self.next_id
                    self.next_id += 1
                
            # add event_types if requested
            if self.event_types != "none":
                for et in self.event_types:
                    self.symbol_to_id[et] = self.next_id
                    self.next_id += 1
            
            # add factors if requested
            if self.factors != "none":
                for fact in self.factors:
                    if fact not in self.symbol_to_id:
                        self.symbol_to_id[fact] = self.next_id
                        self.next_id += 1
            
            # add times if requested
            if self.times != "none":

                # add exact time bins if exact time passage method chosen
                if self.time_passage == "exact":
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

                # handle never case
                else:
                    # should not get here
                    raise ValueError("Tokenizer.build_vocab: self.time_passage is neither exact nor approximate even though times is not none (how did this pass __init__!?).")

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
        pass

    def detokenize(self, tokens):
        """
        Args:
            ...
        Returns:
            events (List<Dict>): |patient_id|code|time|end|numeric_value|text_value|unit|event_type|visit_id| (single patient)
        """
        pass

class Textualizer():
    pass