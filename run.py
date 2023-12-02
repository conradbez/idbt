from idbt.idbt import DbtProject, Node

nodes = [
    Node(name='select', type='select', upstream=["data"], simple_settings={}),
]

dbt = DbtProject(user_inputted_nodes=nodes)
print(f'dbt models {dbt.dbt_models}')
dbt.run_seed()
dbt.compile_models()
dbt.run_project()