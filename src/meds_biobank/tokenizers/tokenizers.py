from meds_biobank.ontologies.ontologies import Ontology

class Tokenizer():

    # TODO: implement body

    # TODO: add option to perform heirarchichal tokenization of codes based on domain ancestors (allow specification of event types)

    def __init__(
        self,
        ontology,
        return_qualifiers=False,
        return_event_types=False,
        return_times=False,
        time_method=None
    ):
        """
        Args:
            ontology (object.Ontology):
                Desc: source ontology
            return_qualifiers (bool):
                Desc: whether to return qualifier concepts
            return_event_types (bool):
                Desc: whether to return event types
            return_times (bool):
                Desc: whether to return times
            time_method (string): exact, approximate
                Desc: if set, method to use for tokenizing times, otherwise simply return times
        """

        # basic guard
        if not isinstance(ontology, Ontology):
            raise Exception("Error: tokenizer.__init__(): ontology must be an Ontology object.")

        # ensure time_method is not set without return_times True
        if time_method is not None and return_times is False:
            raise Exception("Error: tokenizer.__init__(): time_method is set even though return_times is False.")

        # set instance variables
        self.ontology = ontology
        self.return_qualifiers = return_qualifiers
        self.return_event_types = return_event_types
        self.return_times = return_times
        self.time_method = time_method

        # tracking var
        self.last_assigned_id = -1

        # init fields
        self.symbols = None
        self.symbol_to_id = None
        self.id_to_symbol = None
        

    def build_vocab(self):

        # init vars
        self.symbols = set()
        self.symbol_to_id = {}
        self.id_to_symbol = {}

        try:

            # record symbol types to assign tokenizer ids for
            symtypes = [
                "codes",
                "bins",
                "units"
            ]

            # if return_qualifiers is True, add qualifier symbols
            if self.return_qualifiers:
                symtypes.append("qualifiers")

            # if return_event_types is True, add event type symbols
            if self.return_event_types:
                symtypes.append("event_types")

            # add all basic symbols from ontology
            for st in sytypes:
                symbols = getattr(self.ontology, st)
                for symbol in symbols:
                    self.symbols.add(symbol)
                    self.symbol_to_id[symbol] = self.last_assigned_id
                    self.last_assgined_id += 1

            # TODO: if time_method is set, add time symbols to vocab
            if self.time_method == "exact":
                pass
            if self.time_method = "approximate":
                pass
            
            # create id to symbol from symbol to id
            self.id_to_symbol = {v:k for k,v in self.symbol_to_id.items()}
        
        except:
            self.symbols = None
            self.symbol_to_id = None
            self.id_to_symbol = None

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