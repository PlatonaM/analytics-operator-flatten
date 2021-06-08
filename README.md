## analytics-operator-flatten

Extracts nested data elements and creates a flat structure by counting occurrences of nested elements.

### Configuration

`field_patterns`: Provide fields containing nested data, and the naming scheme to use for these fields: `root_field_A:A_sub1_{sub1}_sub2_{sub2},root_field_B:B_sub1_{sub1}_sub2_{sub2}`.

`target_input`: Input containing data (JSON) to flatten.

`logging_level`: Set logging level to `info`, `warning`, `error` or `debug`.

### Inputs

User defined via GUI. The input containing data to flatten must be identified via the `target_input` config option. 
Other inputs will be relayed.

### Outputs

`flat_data`: Data converted to flat structure (JSON).

`default_values`: Default values for newly generated fields (JSON).

Relayed inputs.
