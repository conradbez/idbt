Classes:

Node
attributes
- id
- name
- type (NodeTypeLiteral)
- settings (simple_settings)
- upstream (List[str])
methods
- (constructor)
ModelParamProcessing
methods
- call (processing logic)
- select (node processing)
- merge (node processing)
- append (node processing)
- filter (node processing)
DbtModel
attributes
- node (Node)
- upstream_models (List[Node])
- template_name (str)
- modelParamProcessing (ModelParamProcessing)
methods
- (constructor)
- get_df
- get_columns
- compile
DbtProject
attributes
- user_inputted_nodes (List[Node])
- yaml_template
- dbt_models (List[DbtModel])
methods
- (constructor)
- call (invokes run_seed, compile_models, run_project)
- generate_model_list
- run_project
- run_seed
- compile_models