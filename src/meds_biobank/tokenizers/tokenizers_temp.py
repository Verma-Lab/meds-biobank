class Tokenizer():

    # TODO: implement

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
        pass

    def build_vocab(self):
        pass

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