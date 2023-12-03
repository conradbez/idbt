# A python script that takes a diagram dict and builds a dbt project with these classes
from typing import Literal, List, Dict, get_args
from dbt.cli.main import dbtRunner, dbtRunnerResult
from mako.template import Template
import os
import glob
from pathlib import Path
DBT_PROJECT_NAME = 'dbt_project'
IDBT_DIR = os.path.dirname(os.path.abspath(__file__))
DBT_CLI_ARGS =  ["--project-dir", f"{IDBT_DIR}/dbt_project", "--profiles-dir", IDBT_DIR]
IDBT_MODEL_DIR = os.path.join(IDBT_DIR, 'dbt_project', 'models', 'idbt',)
print(IDBT_DIR)
dbt = dbtRunner()

TEMPLATE_NAMES = [os.path.splitext(os.path.basename(f))[0] for f in glob.glob("/Users/conrad/idbt2/idbt/src/idbt/templates/*", )]
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

# Define the type for a list of nodes and data sources
ListOfNodes = List[Node]

class ModelParamProcessing:
    """Glue function that takes the node for each node type and returns the correct settings for the template"""
    def __call__(self, node: Node):
        if not self.__getattribute__(node.type):
            raise NotImplementedError(f"Model type {node.type} not implemented")
        return self.__getattribute__(node.type)(node)
        
    def select(self, node: Node):
        assert len(node.upstream)==1
        return {'table1': node.upstream[0]}
    def merge(self, node: Node):
        assert len(node.upstream)==2
        return {'table1': node.upstream[0], 'table2': node.upstream[1]}
    def append(self, node: Node):
        assert len(node.upstream)==2
        return {'table1': node.upstream[0], 'table2': node.upstream[1]}
    def filter(self, node: Node):
        where_clause = node.settings.get('where_clause', False)
        assert where_clause
        return {'table1': node.upstream[0], 'where_clause': where_clause}
           

class DbtModel:
    def __init__(self, node: Node,  template_name, modelParamProcessingOverride: ModelParamProcessing = None):
        self.node = node
        self.upstream_models = node.upstream
        self.template_name = template_name
        if modelParamProcessingOverride == None:
            # Not extended so we're using the default class
            self.modelParamProcessing = ModelParamProcessing()
        else:
            # User extended the class so we're using provided class to 
            # translate the node inputs
            self.modelParamProcessing = modelParamProcessingOverride()
    
    def get_columns(self):
        # Implementation to get columns
        pass

    def compile(self):
        # Delete previous run models
        previous_run_sql_files = glob.glob(str(IDBT_MODEL_DIR) + '/*sql')
        for previous_run_sql_file in previous_run_sql_files:
            os.remove(previous_run_sql_file)
        # Implementation to compile model
        template_path = os.path.join(IDBT_DIR, 'templates', f'{self.template_name}.sql')
        with open(template_path, "r") as file:
            template_content = file.read()
            compiled_sql = Template(template_content).render(**self.modelParamProcessing(node=self.node))
            
        compiled_sql_file_path = os.path.join(IDBT_MODEL_DIR, f'{self.node.name}.sql')
        with open(compiled_sql_file_path, "w", ) as file:
            file.write(compiled_sql)

class DbtProject:
    def __init__(self,  user_inputted_nodes: List[Node]):
        self.user_inputted_nodes = user_inputted_nodes
        self.yaml_template = None
        self.dbt_models = self.generate_model_list(user_inputted_nodes) 

    def generate_model_list(self, user_inputted_nodes: ListOfNodes) -> ListOfNodes:
        len_inputted = len(user_inputted_nodes)
        user_inputted_nodes = list(filter(lambda n: n.type in TEMPLATE_NAMES, user_inputted_nodes))
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
        cli_args = ["seed"] + DBT_CLI_ARGS
        res: dbtRunnerResult = dbt.invoke(cli_args)
        return res
    
    def compile_models(self):
        print('compiling models')
        print([m.compile() for m in self.dbt_models])

if __name__ == "__main__":
    pass