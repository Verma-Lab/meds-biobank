from meds_biobank.ontologies.ontologies import Ontology
import math
from datetime import datetime, timezone, timedelta

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
            events (List<Dict>):
                Description:
                    events for a single patient
                Dict Schema:
                    patient_id: int
                    code: int
                    time: datetime
                    end: datetime/null
                    numeric_value: float/null
                    text_value: string/null
                    unit: string/null
                    event_type: string
                    visit_id: int/null
        Returns:
            tokens (Dict):
                Description:
                    dict of token lists for codes and annotations
                Schema:
                    codes (List<int>): ...
                    times (List<datetime>): ...
                    qualifiers (List<int>): ...
                    event_types (List<int>): ...
                    factors (List<int>): ...
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
            tokens (Dict):
                Description:
                    dict of token lists for codes and annotations
                Schema:
                    codes (List<int>): ...
                    times (List<datetime>): ...
                    qualifiers (List<int>): ...
                    event_types (List<int>): ...
                    factors (List<int>): ...
        Returns:
            events (List<Dict>):
                Description:
                    events for a single patient
                Dict Schema:
                    patient_id: int
                    code: int
                    time: datetime
                    end: datetime/null
                    numeric_value: float/null
                    text_value: string/null
                    unit: string/null
                    event_type: string
                    visit_id: int/null
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
    def __init__(self, base_tokenizer, method="approximate"):

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
        self.time_symbols = None

    def augment_vocab(self):

        # init time symbols
        self.time_symbols = set()

        # add exact time bins if exact time passage method chosen
        if self.method == "exact":
            for i in range(60):
                symb = f"minutes_{i}"
                self.time_symbols.add(symb)
                self.base_tokenizer.symbols.add(symb)
                self.base_tokenizer.symbol_to_id[symb] = self.base_tokenizer.next_id
                self.base_tokenizer.id_to_symbol[self.base_tokenizer.next_id] = symb
                self.base_tokenizer.next_id += 1
            for i in range(24):
                symb = f"hours_{i}"
                self.time_symbols.add(symb)
                self.base_tokenizer.symbols.add(symb)
                self.base_tokenizer.symbol_to_id[symb] = self.base_tokenizer.next_id
                self.base_tokenizer.id_to_symbol[self.base_tokenizer.next_id] = symb
                self.base_tokenizer.next_id += 1
            for i in range(7):
                symb = f"days_{i}"
                self.time_symbols.add(symb)
                self.base_tokenizer.symbols.add(symb)
                self.base_tokenizer.symbol_to_id[symb] = self.base_tokenizer.next_id
                self.base_tokenizer.id_to_symbol[self.base_tokenizer.next_id] = symb
                self.base_tokenizer.next_id += 1
            for i in range(52):
                symb = f"weeks_{i}"
                self.time_symbols.add(symb)
                self.base_tokenizer.symbols.add(symb)
                self.base_tokenizer.symbol_to_id[symb] = self.base_tokenizer.next_id
                self.base_tokenizer.id_to_symbol[self.base_tokenizer.next_id] = symb
                self.base_tokenizer.next_id += 1
            for i in range(100):
                symb = f"years_{i}"
                self.time_symbols.add(symb)
                self.base_tokenizer.symbols.add(symb)
                self.base_tokenizer.symbol_to_id[symb] = self.base_tokenizer.next_id
                self.base_tokenizer.id_to_symbol[self.base_tokenizer.next_id] = symb
                self.base_tokenizer.next_id += 1
        
        # add approximate time bins if approximate time passage method chosen
        elif self.time_passage == "approximate":
            time_symbols = ["minutes_5-minutes_15", "minutes_15-hours_1", "hours_1-hours_2", "hours_2-hours_6", "hours_6-hours_12", "hours_12-days_1", "days_1-days_3", "days_3-weeks_1", "weeks_1-weeks_2", "weeks_2-months_1", "months_1-monnths_3", "months_3-months_6", "months_6"]
            for symb in time_symbols:
                self.time_symbols.add(symb)
                self.base_tokenizer.symbols.add(symb)
                self.base_tokenizer.symbol_to_ids[symb] = self.ontology.next_id
                self.base_tokenizer.id_to_symbol[self.base_tokenizer.next_id] = symb
                self.base_tokenizer.next_id += 1
    
    def tokenize(self, events):
        """
        Args:
            events (List<Dict>):
                Description:
                    events for a single patient
                Dict Schema:
                    patient_id: int
                    code: int
                    time: datetime
                    end: datetime/null
                    numeric_value: float/null
                    text_value: string/null
                    unit: string/null
                    event_type: string
                    visit_id: int/null
        Returns:
            tokens (Dict):
                Description:
                    dict of token lists for codes and annotations
                Schema:
                    codes (List<int>): ...
                    qualifiers (List<int>): ...
                    event_types (List<int>): ...
                    factors (List<int>): ...
        """

        # compute base tokens
        temp_tokens = self.base_tokenizer.tokenize()

        # inject time tokens into code tokens based on chosen method
        tokens = {
            "codes": []
        }
        if self.qualifiers:
            tokens["qualifiers"] = []
        if self.event_types:
            tokens["event_types"] = []
        if self.factors:
            tokens["factors"] = []
        
        # hold last time
        last_time = None
        
        # iterate through original token structure
        for i in range(len(temp_tokens["codes"])):

            # skip time tokenization if first iteration
            if last_time is not None:

                # compute time passage since last code
                time_passage = temp_tokens["times"][i] - last_time

                # compute time tokens
                time_tokens = self.tokenize_time_diff(time_diff)

                # if there are any time tokens, insert into codes and rollout metadata
                if len(time_tokens) > 0:
                    for i, tok in enumerate(time_tokens):
                        tokens["codes"].append(tok)
                        if self.qualifiers:
                            tokens["qualifiers"].append(None)
                        if self.event_types:
                            tokens["event_types"].append(None)
                        if self.factors:
                            tokens["factors"].append(None)
            last_time = temp_tokens["times"][i]

            # copy code, metadata
            tokens["codes"].append(temp_tokens["codes"][i])
            if self.qualifiers:
                tokens["qualifiers"].append(temp_tokens["qualifiers"][i])
            if self.event_types:
                tokens["event_types"].append(temp_tokens["event_types"][i])
            if self.factors:
                tokens["factors"].append(temp_tokens["factors"][i])

        return tokens

    def detokenize(self, tokens):
        """
        Args:
            tokens (Dict):
                Description:
                    dict of token lists for codes and annotations
                Schema:
                    codes (List<int>): ...
                    qualifiers (List<int>): ...
                    event_types (List<int>): ...
                    factors (List<int>): ...
        Returns:
            events (List<Dict>):
                Description:
                    events for a single patient
                Dict Schema:
                    patient_id: int
                    code: int
                    time: datetime
                    end: datetime/null
                    numeric_value: float/null
                    text_value: string/null
                    unit: string/null
                    event_type: string
                    visit_id: int/null
        """
        
        # use injected time tokens to assign times to basic events
        start_time = datetime.fromtimestamp(0, tz=timezone.utc) # 1970-01-01 00:00:00+00:00
        temp_tokens = {
            "codes": [],
            "times": [],
        }
        if self.qualifiers:
            temp_tokens["qualifiers"] = []
        if self.event_types:
            temp_tokens["event_types"] = []
        if self.factors:
            temp_tokens["factors"] = []
        
        # init var to store growing time token list
        temp_time_tokens = []

        # iterate through codes
        for i in range(len(tokens["codes"])):

            # grab code token and code
            code_token = tokens["codes"][i]
            code = self.base_tokenizer.id_to_symbol[code_token]

            # handle BOS
            if (code == "BOS"):
                temp_tokens["codes"].append(code_token)
                temp_tokens["times"].append(None)
                if self.qualifiers:
                    temp_tokens["qualifiers"].append(None)
                if self.event_types:
                    temp_tokens["event_types"].append(None)
                if self.factors:
                    temp_tokens["factors"].append(None)
            
            # handle time symbol
            elif code in self.time_symbols:
                temp_time_tokens.append(code_token)
            
            # handle regular code
            else:

                # save code
                temp_tokens["codes"].append(code_token)

                # compute previous time
                previous_time = (None or start_time)

                # if we have built up time tokens to detokenize, detokenize and add time to last available to get current time
                if len(temp_time_symbols) > 0:

                    # compute time passage
                    tdelt = self.detokenize_time_diff(temp_time_tokens)

                    # then reset temp time tokens
                    temp_time_tokens = []

                    # get current time
                    current_time = previous_time + tdelt
                
                # otherwise, propagate previous time
                else:
                    current_time = previous_time
                
                # set time of token
                temp_tokens["times"].append(current_time)

                # save metadata
                if self.qualifiers:
                    temp_tokens["qualifiers"].append(tokens["qualifiers"][i])
                if self.event_types:
                    temp_tokens["event_types"].append(tokens["event_types"][i])
                if self.factors:
                    temp_tokens["factors"].append(tokens["factors"][i])

        return self.base_tokenizer.detokenize(temp_tokens)
    
    def tokenize_time_diff(self, time_diff, floor="minutes_5", cieling="years_10"):
        """
        Args:
            time_diff (timedelta): ...
        Returns:
            tokens (List<int>): ...
        """

        # return empty list if the time difference is too short to tokenize
        floor_tdelt = self.code_to_tdelt(floor)
        too_short = (time_diff < floor_tdelt)
        if too_short:
            return []
        
        # if time_diff larger than cieling, cap it
        cieling_tdelt = self.code_to_tdelt(cieling)
        too_long = (cieling_tdelt < time_dff)
        if too_long:
            time_diff = cieling_tdelt

        # if exact method is chosen
        if self.method == "exact":

            # compute num years
            total_days = time_diff.days
            years = int((total_days - (total_days % 365)) / 365)

            # compute num weeks from leftover days
            leftover_days = total_days - years * 356
            weeks = int((leftover_days - (leftover_days % 7)) / 7)

            # compute num days from leftover days
            days = leftover_days - weeks * 7

            # read hours, minutes directly
            hours = time_diff.hours
            minutes = time_diff.minutes

            # form string codes
            years_code = f"years_{years}"
            weeks_code = f"weeks_{weeks}"
            days_code = f"days_{days}"
            hours_code = f"hours_{hours}"
            minutes_code = f"minutes_{minutes}"

            # tokenize codes
            codes = [years_code, weeks_code, days_code, hours_code, minutes_code]
            codes = [code for code in codes if code not None]
            time_tokens = [self.base_tokenizer.symbol_to_id[code] for code in codes]

            return time_tokens
        
        # if approximate method is chosen
        elif self.method == "approximate":

            # attempt to locate the bin for the time symbol
            for tsymb in self.time_symbols:
                lower_code, upper_code = tsymb.split("-")
                lower_tdelt = self.code_to_tdelt(lower_code)
                upper_tdelt = self.code_to_tdelt(upper_code)
                if (lower_tdelt <= time_diff and time_diff < upper_tdelt):
                    return self.ontology.symbol_to_id[tsymb]

            # if we no bin is located, approximate the time with 6 mo tokens and return list
            time_tokens = []
            temp_time_diff = time_diff
            sixmo = timedelta(days=(30.5*6))
            while temp_time_diff > sixmo:
                temp_time_diff -= sixmo
                time_tokens.append(self.ontology.symbol_to_id["=months_6"])
            threemo = timedelta(days=(30.5*3))
            if time_diff > threemo:
                time_tokens.append(self.ontology.symbol_to_id["=months_6"])
            return time_tokens
        else:
            raise ValueError(f"TimeTokenizer.tokenize_time_diff(): invalid time passage method {self.method} recieved. How did we get here? This should have been caught by __init__().")
    
    def detokenize_time_diff(self, time_tokens):
        """
        Args:
            tokens (List<int>): ...
        Returns:
            time_diff (timedelta): ...
        """

        # handle case where there are no tokens
        if len(time_tokens) == 0:
            return timedelta(days=0)

        # translate time tokens to time codes
        time_codes = [self.base_tokenizer.id_to_symbol[token] for token in time_tokens]

        # handle exact conversion
        if method == "exact":

            # init time diff to 0
            time_diff = timedelta(days=0)

            # add every token's diff to total then return
            for tc in time_codes:
                time_diff += self.code_to_tdelt(tc)
            return time_diff
        
        # handle approxiumate method
        elif method == "approximate":

            # if the time token consists of a single range token
            if len(time_codes) == 1:
                time_code = time_codes[0]
                lower, upper = time_code.split("-")
                lower_tdelt = self.code_to_tdelt(lower)
                upper_tdelt = self.code_to_tdelt(upper)
                return (lower_tdelt + upper_tdelt) / 2
            
            # otherwise it consists of multiple six-month tokens
            else:
                return timedelta(days=(30.5 * 6 * len(time_codes)))
        
        # handle unspecified method
        else:
            raise ValueError()
    
    def code_to_tdelt(self, code):
        """
        Args:
            code (string):
                Description:
                    one of: minutes_i, hours_i, days_i, weeks_i, months_i, years_i
        Returns:
            tdelt (timedelta): time delta representing equivalent passage of time
        """

        # convert code to timedelta
        unit, magnitude = code.split("_")
        try:
            maginitude = int(magnitude)
        except:
            raise ValueError()
        if unit == "minutes":
            return timedelta(minutes=magnitude)
        elif unit == "hours":
            return timedelta(hours=magnitude)
        elif unit == "days":
            return timedelta(days=magnitude)
        elif unit == "weeks":
            return timedelta(days=7*magnitude)
        elif unit == "months":
            return timedelta(days=(30.5)*magnitude)
        elif unit == "years":
            return timedelta(days=(365)*magnitude)
        else:
            raise ValueError()

class RolloutTokenizer():
    def __init__(self, tokenizer, qualfiers=False, event_types=False, factors=True):
        
        # make sure fields have proper types and settings
        if not isinstance(tokenizer, BaseTokenizer) and not isinstance(tokenizer, TimeTokenizer):
            raise ValueError()
        if not (qualifiers or event_types or factors):
            raise ValueError()
        
        # make sure base tokenizer is properly initialized
        if isinstance(tokenizer, TimeTokenizer):
            if tokenizer.time_symbols is None:
                raise ValueError()
            if None in (tokenizer.base_tokenizer.symbols, tokenizer.base_tokenizer.symbol_to_id, tokenizer.base_tokenizer.id_to_symbol):
                raise ValueError()
        if isinstance(tokenizer, BaseTokenizer):
            if None in (tokenizer.symbols, tokenizer.symbol_to_id, tokenizer.id_to_symbol):
                raise ValueError()
        
        # init fields
        self.base_tokenizer = base_tokenizer
        self.is_time_tokenizer = (isinstance(tokenizer, TimeTokenizer))
        self.qualifiers = qualifiers
        self.event_types = event_types
        self.factors = factors
    
    def tokenize(events):
        
        # compute base tokens
        base_tokens = self.base_tokenizer.tokenize(events)

        # TODO: rollout requested fields
        unrolled_tokens = ...

        return unrolled_tokens
    
    def detokenize(tokens):

        # TODO: roll requested fields
        rolled_tokens = ...
        
        return self.base_tokenizer.detokenize(rolled_tokens)

class Textualizer():
    pass