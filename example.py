from idbt.idbt import DbtProject, Node

nodes = [
    Node(name='idbt_select_table', type='select', upstream=["data"], simple_settings={}),
    Node(name='idbt_filter_table', type='filter', upstream=["idbt_select_table"], simple_settings={'where_clause': "1"}),
]

dbt = DbtProject(user_inputted_nodes=nodes)

dbt.run_seed()
dbt.compile_models()
dbt.run_project()
print(dbt.dbt_models[0].get_df(method='show'))