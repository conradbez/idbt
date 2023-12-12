# A python script that takes a diagram dict and builds a dbt project with these classes
from typing import Any, Literal, List, Dict, get_args
from dbt.cli.main import dbtRunner, dbtRunnerResult
from mako.template import Template
import os
import glob
from pathlib import Path
import duckdb
from idbt.settings import IDBT_DIR, DUCK_DB_PATH, DBT_CLI_ARGS, IDBT_MODEL_DIR, TEMPLATE_NAMES, DATA_SOURCE_NAMES, DELTA_RUN_CLI_ARGS
import streamlit as st
# Flow
# 1. DbtNode class:User inputs a diagram
# 2. DbtModel class: Parse the nodes to dbt models
# 3. DbtProject class: Compile the nodes into dbt models

dbt = dbtRunner()

NodeTypeLiteral = Literal['append', 'merge', 'select', 'filter']

class Node:
    def __init__(self, name: str, type: NodeTypeLiteral, upstream : List[str], simple_settings: Dict[str, str]):
        """
        Abstraction of a node, still un user land we represent what the user has inputted
        To then start moving towards dbt constructs (DbtModel)

        simple_settings: settings which will just get passed directly to the template
        """
        self.id = name
        self.name = name
        self.type = type
        self.settings = simple_settings
        self.upstream = upstream
    
    def modelParamProcessing(self) -> Dict[str, Any]:
        return {}
    
    
# Define the type for a list of nodes and data sources
ListOfNodes = List[Node]

class MergeNode(Node):
    def modelParamProcessing(self):
        assert len(self.upstream)==2
        return {'table1': self.upstream[0], 'table2': self.upstream[1], 'merge_col' : self.settings['merge_column']}
class AppendNode(Node):    
    def modelParamProcessing(self):
        assert len(self.upstream)==2
        return {'table1': self.upstream[0], 'table2': self.upstream[1]}
class FilterNode(Node):    
    def modelParamProcessing(self: Node):
        filter_where_clause = self.settings.get('filter_where_clause', False)
        st.sidebar.write(self.settings)
        assert filter_where_clause
        return {'table1': self.upstream[0], 'where_clause': filter_where_clause}
class SelectNode(Node):
    def modelParamProcessing(self: Node):
        st.sidebar.write(self.settings)
        try:
            assert len(self.upstream)==1
        except AssertionError as e:
            print(f"Node {self.name} has {len(self.upstream)} upstream nodes, but should have 1")
            raise e
        
        if not self.settings.get('select_columns', False): 
            # for testing flow through
            with st.expander('No node settings found, using default select columns for '):
                # st.warning('No node settings found, using default select columns for ')
                st.write(self)
        select_columns = self.settings.get('select_columns', '*') 
        return {'table1': self.upstream[0], 'select_columns': select_columns}

class DbtModel:
    def __init__(self, node: Node,  template_name):
        self.node = node
        self.template_name = template_name

    def get_df(self, method: Literal['df', 'show', 'fetchall'] = 'show'):
        con = duckdb.connect(DUCK_DB_PATH)
        res = con.sql(f'''SELECT * from "{self.node.name}"''')
        return res.__getattribute__(method)()

    def get_columns(self):
        return self.get_df(method='df').columns

    def compile(self):
        # Implementation to compile model
        template_path = os.path.join(IDBT_DIR, 'templates', f'{self.template_name}.sql')
        with open(template_path, "r") as file:
            template_content = file.read()
            compiled_sql = Template(template_content).render(**self.node.modelParamProcessing())
        compiled_sql_file_path = os.path.join(IDBT_MODEL_DIR, f'{self.node.name}.sql')
        with open(compiled_sql_file_path, "w", ) as file:
            file.write(compiled_sql)

class DbtProject:
    def __init__(self,  user_inputted_nodes: List[Node]):
        self.user_inputted_nodes = user_inputted_nodes
        self.yaml_template = None
        self.dbt_models = self.generate_model_list(user_inputted_nodes) 

    def full_clean(self):
        """delete db, model files"""
        self.delete_db()
        previous_run_sql_files = glob.glob(str(IDBT_MODEL_DIR) + '/*sql')
        for previous_run_sql_file in previous_run_sql_files:
            os.remove(previous_run_sql_file)
        clean_res: dbtRunnerResult = dbt.invoke(["clean",] + DBT_CLI_ARGS)
        return clean_res
    
    def delete_db(self):
        try:
            os.remove(DUCK_DB_PATH)
        except:
            print('no database found to delete on full clean')

    def __call__(self, *args: Any, **kwds: Any) -> Any:
        self.compile_models()
        res: dbtRunnerResult = self.run_project()
        return res


    def get_model_by_id(self, node_id: str) -> DbtModel | None:
        selected_model = [m for m in  self.dbt_models if m.node.id==node_id]
        if len(selected_model)==0:
            return None
        assert len(selected_model)==1, Exception(f"Error on getting upstream of {node_id}")

        return selected_model[0]

    def generate_model_list(self, user_inputted_nodes: ListOfNodes) -> List[DbtModel]:
        len_inputted = len(user_inputted_nodes)
        user_inputted_nodes = list(filter(lambda n: n.type in TEMPLATE_NAMES, user_inputted_nodes))
        try:
            assert (len(user_inputted_nodes) > 0), "No models found in diagram"
            assert len(user_inputted_nodes) == len_inputted, "Some nodes are not valid models"
        except AssertionError as e:
            print(TEMPLATE_NAMES)
            print(e)
            raise e
        return [DbtModel(
            n,
            n.type, # template name is just the model type atm
            ) for n in user_inputted_nodes]
    
    def run_project(self, select : str = None, delta_run=False):
        # if delta_run:
        #     cli_args = ["run",] + DELTA_RUN_CLI_ARGS
        # else:
        if not select==None:
            cli_args = ['--select', select] + DBT_CLI_ARGS
            print('running selected models')
            print(cli_args)
        else:
            cli_args = DBT_CLI_ARGS

        cli_args = ["run",] + cli_args
        
        res: dbtRunnerResult = dbt.invoke(cli_args)
        
        try:
            assert res.success
        except:
            raise Exception(f"dbt run failed with {res}")
        return res

    def run_seed(self):
        cli_args = ["seed"] + DBT_CLI_ARGS
        res: dbtRunnerResult = dbt.invoke(cli_args)
        return res
    
    def compile_models(self):
        print('compiling models')
        print([m.compile() for m in self.dbt_models])

if __name__ == "__main__":
    pass