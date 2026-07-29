class Tokenizer():
    def __init__(self, ontology, ontology_rollout=False, domain_rollout=False, qualifier_rollout=False):
        self.ontology = ontology # ontology object
        self.ontology_rollout = ontology_rollout # whether to use ontology rollout
        self.domain_rollout = domain_rollout # wither to use domain rollout
        self.qualifier_rollout = qualifier_rollout # whether to use qualifier rollout
    def build_vocab(self):
        """
        Use ontology to create vocab
        """
        pass
    def tokenize(self, events):
        """
        Args:
            events (List<Dict>): |patient_id|code|time|end|numeric_value|text_value|unit|event_type|visit_id| (single patient)
        Returns:
            tokens (List<int>), times (List<timestamp>), visits (List<int>)
        """
        pass
    def detokenize(self, events):
        """
        Args:
            tokens (List<int>), times (List<timestamp>), visits (List<int>)
        Returns:
            events (List<Dict>): |patient_id|code|time|end|numeric_value|text_value|unit|event_type|visit_id| (single patient)
        """
        pass