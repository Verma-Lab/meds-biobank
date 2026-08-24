from meds_biobank.ontologies.ontologies import Ontology
import math
from datetime import datetime, timezone, timedelta
import pyspark.sql.functions as F

SPECIAL_TOKENS = ["BOS", "BOE"]

class BaseTokenizer():

    def __init__(
        self,
        ontology,
        qualifiers=False,
        event_types=False,
        factors=False,
        units=True,
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
        self.units = units

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
            
            # add numeric bins
            for bin in self.ontology.bins:
                self.symbol_to_id[bin] = self.next_id
                self.next_id += 1
            
            # add textual bins
            for text in self.ontology.common_text_values:
                self.symbol_to_id[text] = self.next_id
                self.next_id += 1

            # add special tokens
            for st in SPECIAL_TOKENS:
                self.symbol_to_id[st] = self.next_id
                self.next_id += 1

            # add qualifiers if requested
            if self.qualifiers:
                for qual in self.ontology.qualifiers:
                    self.symbol_to_id[qual] = self.next_id
                    self.next_id += 1

            # add event_types if requested
            if self.event_types:
                for et in self.ontology.event_types:
                    self.symbol_to_id[et] = self.next_id
                    self.next_id += 1

            # add factors if requested
            if self.factors:
                for fact in self.ontology.factors:
                    if fact not in self.symbol_to_id:
                        self.symbol_to_id[fact] = self.next_id
                        self.next_id += 1

            # add units if requested
            if self.units:
                for unit in self.ontology.units:
                    self.symbol_to_id[unit] = self.next_id
                    self.next_id += 1

            # build id_to_symbol from symbol_to_id
            self.id_to_symbol = {v:k for k,v in self.symbol_to_id.items()}
        
        except:

            # de-init tokenizer fields in case of failure (then raise error)
            self.symbols = None
            self.symbol_to_id = None
            self.id_to_symbol = None
            self.next_id = 0
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
                    codes (List<int>): sequence of primary and decile code tokens
                    times (List<datetime>): sequence of times for primary and decile tokens (decile token time = associated msmt_concept time), None for BOS
                    qualifiers (List<List<int>>): sequence of lists of qualifier tokens, one list per event
                    event_types (List<int>): sequence of event type tokens same length as codes, None for decile tokens
                    factors (List<List<int>>): sequence of lists of factor tokens same length as codes, can be None or empty list so be careful
        """

        if None in (self.symbols, self.symbol_to_id, self.id_to_symbol):
            raise ValueError("BaseTokenizer.tokenize(): symbols, symbol_to_id, and/or id_to_symbol are not initialized. Did you run Tokenizer.build_vocab() yet?")

        # init token list, add BOS
        tokens = {
            "codes": [self.symbol_to_id["BOS"]],
            "times": [None],
            "visit_ids": [None]
        }
        if self.qualifiers:
            tokens["qualifiers"] = [None]
        if self.event_types:
            tokens["event_types"] = [None]
        if self.factors:
            tokens["factors"] = [None]
        
        # init tracking vars
        next_visit_id = 0
        visit_id_map = dict()

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
                code_token = self.symbol_to_id[code]
            
            # if we do not know the code token, skip this event (continue)
            if code_token is None:
                continue
            
            # add token to growing list
            tokens["codes"].append(code_token)

            # if code is a msmt code, handle value
            num_extra_tokens = 0
            value_token = None
            unit_token = None
            if (self.ontology.code_to_event_type[code] == "measurement"):

                # extract numeric value field
                numeric_value = event["numeric_value"]

                # extract unit field
                unit = event["unit"]

                # extract text value field
                text_value = event["text_value"]

                # attempt to tokenize numeric value and unit
                if (code in self.ontology.code_to_bin_ranges) and (numeric_value is not None) and (unit is not None): # if we have deciles for this code, and a value and a unit
                    if unit in self.ontology.code_to_bin_ranges[code]: # if we have deciles for that unit
                        nval = float(numeric_value)
                        if (nval <= 0.0):
                            value_token = self.symbol_to_id["bin_0"]
                        else:
                            nval = math.log1p(nval)
                            for bin_num, value_range in self.ontology.code_to_bin_ranges[code][unit].items():
                                mini = float(value_range["min"])
                                maxi = float(value_range["max"])
                                if (mini <= nval) and (nval < maxi):
                                    break
                            value_token = self.symbol_to_id[f"bin_{bin_num}"]
                        unit_token = self.symbol_to_id[unit]

                # if this fails, attempt to tokenize text value
                if (value_token is None):
                    if (text_value in self.ontology.common_text_values):
                        value_token = self.symbol_to_id[text_value]

                # if this still fails, no value or unit tokens
            
            # now add tokens to the rollout
            if (value_token is not None):
                tokens["codes"].append(value_token)
                num_extra_tokens += 1
            if (unit_token is not None):
                tokens["codes"].append(unit_token)
                num_extra_tokens += 1

            # if the event has a visit id
            visit_id = None
            if event["visit_id"] is not None:

                # extract it
                vid = event["visit_id"]

                # check if the visit id is already registered
                if vid in visit_id_map:

                    # if it is, lookup the index
                    visit_id = visit_id_map[vid]
                else:
                    # if not, register it and increment the index
                    visit_id = next_visit_id
                    visit_id_map[vid] = next_visit_id
                    next_visit_id += 1
            
            # add visit id
            tokens["visit_ids"].append(visit_id)
            for _ in range(num_extra_tokens):
                tokens["visit_ids"].append(visit_id)

            # handle times
            tokens["times"].append(event["time"])
            for _ in range(num_extra_tokens):
                tokens["times"].append(event["time"])

            # handle factors if requested
            if self.factors:
                if (self.ontology.code_to_event_type[event["code"]] in self.factor_types) and (code in self.ontology.code_to_factors): # if event type is a requested one for factors and the code has actual factors
                    factor_codes = self.ontology.code_to_factors[code]
                    factor_tokens = []
                    for fc in factor_codes:
                        factor_tokens.append(self.symbol_to_id[fc])
                    tokens["factors"].append(factor_tokens)
                else:
                    tokens["factors"].append(None)
                for _ in range(num_extra_tokens):
                    tokens["factors"].append(None)

            # handle qualifiers if requested
            if self.qualifiers:
                qualifiers = self.ontology.code_to_qualifiers.get(code, [])
                qualifier_tokens = []
                for qual in qualifiers:
                    qualifier_tokens.append(self.symbol_to_id[qual])
                tokens["qualifiers"].append(qualifier_tokens)
                for _ in range(num_extra_tokens):
                    tokens["qualifiers"].append(None)

            # handle event types
            if self.event_types:
                event_type_code = self.ontology.code_to_event_type[event["code"]]
                event_type_token = self.symbol_to_id[event_type_code]
                tokens["event_types"].append(event_type_token)
                for _ in range(num_extra_tokens):
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
            
            # skip beginning of sequence
            if code == "BOS":
                i += 1
                continue
            
            # skip value and unit tokens as they should have been consumed
            if (code in self.ontology.bins) or (code in self.ontology.units) or (code in self.ontology.common_text_values):
                i += 1
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
            event["event_type"] = self.ontology.code_to_event_type[code]

            # handle text value
            if (i <= n-2):
                next_token = tokens["codes"][i+1]
                next_token_symbol = self.id_to_symbol[next_token]
                is_tv = (next_token_symbol in self.ontology.common_text_values)
                if (is_tv):
                    event["text_value"] = next_token_symbol
                    i += 2
                    events.append(event)
                    continue

            # handle numeric value and unit
            if i <= (n-3):

                # check if next token is a decile bin
                next_token = tokens["codes"][i+1]
                next_token_symbol = self.id_to_symbol[next_token]
                is_bin = (next_token_symbol in self.ontology.bins)

                # if it is
                if is_bin:

                    # relabel and consume the unit too
                    bin_code = next_token_symbol
                    unit_token = tokens["codes"][i+2]
                    unit_code = self.id_to_symbol[unit_token]

                    # if code is in bin ranges and unit has bins stored
                    if code in self.ontology.code_to_bin_ranges:
                        if unit_code in self.ontology.code_to_bin_ranges[code]:

                            # get bin
                            bin_int = int(bin_code.split("_")[1])

                            # handle 0 case
                            if (bin_int == 0):
                                event["numeric_value"] = 0.0
                            
                            # if not zero, impute value
                            else:

                                # impute value, unit, increment i by (an extra) 1 (so it increments by two by end)
                                mini = float(self.ontology.code_to_bin_ranges[code][unit_code][bin_int]["min"])
                                maxi = float(self.ontology.code_to_bin_ranges[code][unit_code][bin_int]["max"])
                                event["numeric_value"] = math.expm1((mini+maxi)/2)
                            
                            # set unit
                            event["unit"] = unit_code

                    # consume bin and unit tokens then continue
                    i += 3
                    events.append(event)
                    continue

            # add event to events
            events.append(event)

            # increment counter for manual loop
            i += 1

        return events

class TimeTokenizer():
    def __init__(self, base_tokenizer, method="approximate"):

        # guard against type errors
        if not isinstance(base_tokenizer, BaseTokenizer):
            raise ValueError("TimeTokenizer.__init__(): base_tokenizer is not of class BaseTokenizer.")

        # guard against invalid method arguments
        if method not in {"approximate", "exact"}:
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
            codes = [code for code in codes if code is not None]
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

    # TODO: rework entirely, as factors could be codes and this gets confused. add beginning of event token.

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
        self.tokenizer = tokenizer
        self.qualifiers = qualifiers
        self.event_types = event_types
        self.factors = factors
    
    def tokenize(events):

        # if tokenizer is a base tokenizer, times is full (although time for BOS will be none), but if not, then there are not times
        
        # compute base tokens
        base_tokens = self.tokenizer.tokenize(events)

        # rollout requested fields
        unrolled_tokens = {
            "tokens": []
        }
        if "times" in events:
            unrolled_tokens["times"] = []
        for i in range(len(events["codes"])):

            # if there is a time, capture it and associate with all unrolled tokens (going forward)
            if "times" in events:
                curr_time = events["times"][i]

            # if user requested event types, insert into sequence
            if self.event_types:
                event_type_token == events["event_types"][i]
                if event_type_token is not None:
                    unrolled_tokens["tokens"].append(event_type_token)
                    if "times" in events:
                        unrolled_tokens["times"].append(curr_time)

            # if user requested quals, insert into sequence
            if self.qualifiers:
                qualifier_tokens = events["qualifiers"][i]
                if qualifier_tokens is not None:
                    if len(qualifier_tokens) > 0:
                        unrolled_tokens["codes"].extend(qualifier_tokens)
                        if "times" in events:
                            unrolled_tokens["times"].extend([curr_time for i in range(len(qualifier_tokens))])

            # if there are requested factors, insert them into sequence
            if self.factors:
                factor_tokens = events["factors"][i]
                if factor_tokens is not None:
                    if len(factor_tokens) > 0:
                        unrolled_tokens["codes"].extend(factor_tokens)
                        if "times" in events:
                            unrolled_tokens["times"].extend([curr_time for i in range(len(factor_tokens))])

            # insert the actual code and time
            unrolled_tokens["codes"].append(events["codes"][i])
            if "times" in events:
                unrolled_tokens["times"].append(curr_time)

        return unrolled_tokens
    
    def detokenize(tokens):

        # tokens will have fields: "codes", and optionally "times" (almost all non-na)

        # roll requested fields
        rolled_tokens = {
            "codes": []
        }
        if "times" in tokens:
            rolled_tokens["times"] = []
        if self.event_types:
            rolled_tokens["event_types"] = []
        if self.qualifiers:
            rolled_tokens["qualifiers"] = []
        if self.factors:
            rolled_tokens["factors"] = []
        
        # init loop var
        i = 0

        # iterate through codes
        while i < range(len(tokens["codes"])):

            # extract token and associated code
            token = tokens["codes"][i]
            code = self.tokenizer.id_to_symbol[token]

            # extract time if requested
            if "times" in tokens:
                curr_time = tokens["times"][i]
            
            # process event type token
            if self.event_types:
                if code in self.tokenizer.ontology.event_types:
                    rolled_tokens["event_types"].append(token)
                    i += 1
                    continue
            
            # process qualifier tokens and consume stretch
            if self.qualifiers:
                if code in self.tokenizer.ontology.qualifiers:
                    qualifier_tokens = []
                    while code in self.tokenizer.ontology.qualifiers:
                        qualifier_tokens.append(token)
                        i += 1
                        token = tokens["codes"][i]
                        code = self.tokenizer.id_to_symbol[token]
                    rolled_tokens["qualifiers"].append(qualifier_tokens)
                    continue
            
            # process factor tokens and consume stretch
            if self.factors:
                if code in self.tokenizer.ontology.factors:
                    factor_tokens = []
                    while code in self.tokenizer.ontology.factors:
                        factor_tokens.append(token)
                        i += 1
                        token = tokens["codes"][i]
                        code = self.tokenizer.id_to_symbol[token]
                    rolled_tokens["factors"].append(factor_tokens)
                    continue

            # process primary code and consume stretch, stamp time, and add nones to auxiliary structures
            if code in self.tokenizer.ontology.codes:
                rolled_tokens["codes"].append(token)
                if "times" in tokens:
                    rolled_tokens["times"].append(tokens["times"][i])
                num_codes = len(rolled_tokens["codes"])
                if self.event_types:
                    while len(rolled_tokens["event_types"]) < num_codes:
                        rolled_tokens["event_types"].append(None)
                if self.qualifiers:
                    while len(rolled_tokens["qualifiers"]) < num_codes:
                        rolled_tokens["qualifiers"].append(None)
                if self.factors:
                    while len(rolled_tokens["factors"]) < num_codes:
                        rolled_tokens["factors"].append(None)
                i += 1
                continue
            
            # process time token(s) if these are present
            if isinstance(self.tokenizer, TimeTokenizer):
                if code in self.tokenizer.time_symbols:
                    rolled_tokens["codes"].append(token)
                    if self.event_types:
                        rolled_tokens["event_types"].append(None)
                    if self.qualifiers:
                        rolled_tokens["qualifiers"].ppend(None)
                    if self.factors:
                        rolled_tokens["factors"].append(None)
        
        return self.tokenizer.detokenize(rolled_tokens)

class Textualizer():
    pass

if __name__ == "__main__":

    # imports
    from pyspark.sql import SparkSession
    from pathlib import Path
    from dotenv import load_dotenv
    import os
    import random
    from meds_biobank import schemas

    # set seed
    random.seed(42)

    # setup
    load_dotenv()
    spark = (
        SparkSession
        .builder
        .master("local[2]")
        .appName("meds-biobank:tokenizer")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    # read meds events
    REPO_ROOT = Path(__file__).resolve().parents[3]
    MEDS_DATA_DIR = REPO_ROOT / os.environ["MEDS_DATA_DIR"] / "generated-standard"
    meds_events_path = MEDS_DATA_DIR / "meds_events.parquet"
    meds_events = spark.read.parquet(str(meds_events_path), schema=schemas.MEDS_EVENT_SCHEMA)

    # read ontology
    ontology_data_dir = REPO_ROOT / os.environ["ONTOLOGY_DATA_DIR"] / "generated-standard-rolled"
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
            "visit_id": row["visit_id"]
        } for row in pt_rows.collect()
    ]
    events = events[:15]
    for event in events:
        print(event)
        print("-"*80)

    # tokenize the patient
    tokens = bt.tokenize(events)
    for i in range(len(tokens["codes"])):
        print(f"|{tokens['codes'][i]}|{tokens['times'][i]}|{tokens['event_types'][i]}|{tokens['factors'][i]}|")

    # detokenize the patient
    decoded_events = bt.detokenize(tokens)
    for event in decoded_events:
        print(event)
        print("-"*80)
