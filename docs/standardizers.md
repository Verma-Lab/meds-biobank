# Standardizers

## Workflow

1. Read OMOP measurements table and join with concept ancestor
2. Subset for each mtype via filter on ancestor, additional conditions
3. Apply mtype-specific logic to extract labs and vitals (future: interpret/correct text values)
4. Perform fallback logic for all concepts not covered by mtype
5. UnionByyName all results and return

## Inputs

OMOP measurements

```bash
measurement_id:long
person_id:string
measurement_concept_id:integer
measurement_date:date
measurement_datetime:timestamp
measurement_type_concept_id:integer
operator_concept_id:integer
value_as_number:decimal(10,3)
value_as_concept_id:integer
unit_concept_id:integer
range_low:decimal(24,3)
range_high:decimal(24,3)
visit_occurrence_id:long
measurement_source_value:string
measurement_source_concept_id:integer
unit_source_value:string
value_source_value:string
```

## Outputs

standardized measurements

```bash
"
unit_converted:string
value_converted:double
std_concept_id:long
```
