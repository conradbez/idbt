# A python script that takes a diagram dict and builds a dbt project with these classes
import glob
from typing import Literal, List, Dict, get_args
from dbt.cli.main import dbtRunner, dbtRunnerResult
from mako.template import Template
import os
from pathlib import Path
DBT_PROJECT_NAME = 'dbt_project'
IDBT_DIR = os.path.dirname(os.path.abspath(__file__))
DBT_CLI_ARGS =  ["--project-dir", f"{IDBT_DIR}/dbt_project", "--profiles-dir", IDBT_DIR]
print(IDBT_DIR)
dbt = dbtRunner()

NodeTypeLiteral = Literal['append', 'merge', 'select']

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

# Define the type for a list of nodes and data sources
ListOfNodes = List[Node]

class DbtModel:
    def __init__(self, node: Node,  template_name):
        self.node = node
        self.upstream_models = node.upstream
        self.template_name = template_name

    def get_columns(self):
        # Implementation to get columns
        pass

    def compile(self):
        # Implementation to compile model
        template_path = os.path.join(IDBT_DIR, 'templates', f'{self.template_name}.sql')
        with open(template_path, "r") as file:
            template_content = file.read()
            # Replace ${table1} with self.upstream_models[0]
            # compiled_sql = Template(template_content).render(table1=self.node.upstream[0], table2=self.upstream_models[1])
            compiled_sql = Template(template_content).render(table1=self.node.upstream[0],)
        compiled_sql_file_path = os.path.join(IDBT_DIR, 'dbt_project', 'models', 'idbt', f'{self.template_name}.sql')
        with open(compiled_sql_file_path, "w", ) as file:
            file.write(compiled_sql)

class DbtProject:
    def __init__(self,  user_inputted_nodes: List[Node]):
        self.user_inputted_nodes = user_inputted_nodes
        self.yaml_template = None
        self.dbt_models = self.generate_model_list(user_inputted_nodes) 

    def generate_model_list(self, user_inputted_nodes: ListOfNodes) -> ListOfNodes:
        len_inputted = len(user_inputted_nodes)
        user_inputted_nodes = list(filter(lambda n: n.type in get_args(NodeTypeLiteral), user_inputted_nodes))
        assert (len(user_inputted_nodes) > 0), "No models found in diagram"
        assert len(user_inputted_nodes) == len_inputted, "Some nodes are not valid models"
        return [DbtModel(
            n,
            n.type, # template name is just the model type atm
            ) for n in user_inputted_nodes]
    
    def run_project(self):
        
        cli_args = ["run",] + DBT_CLI_ARGS
        res: dbtRunnerResult = dbt.invoke(cli_args)
        return res

    def run_seed(self):
        # Implementation to run seed
        cli_args = ["seed"] + DBT_CLI_ARGS

        # run the command
        res: dbtRunnerResult = dbt.invoke(cli_args)
        return res
    
    def compile_models(self):
        print('compiling models')
        print([m.compile() for m in self.dbt_models])

if __name__ == "__main__":
    pass