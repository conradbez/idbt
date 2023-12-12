
import streamlit as st
from duckdb import CatalogException

def generic_input_wrap(func,):

    def wrap(*args, **kwargs):
        # get user settings if exists
        node_selected_name = kwargs['node_selected_name']
        st.session_state['node_user_config'] = st.session_state.get('node_user_config', {})
        current_user_config_for_node = st.session_state['node_user_config'].get(node_selected_name,{})
        current_user_config_for_node['settings'] = current_user_config_for_node.get('settings', {})
        settings = current_user_config_for_node['settings']
    
        node_settings = func(settings = settings, *args, **kwargs)

        current_user_config_for_node['settings'] = node_settings
        st.session_state['node_user_config'][node_selected_name] = current_user_config_for_node
        return current_user_config_for_node
    return wrap

@generic_input_wrap
def select_input( *, settings, dbt, node_selected_name): # settings passed by wrapping function
    # get options from model if the model exists
    try:
        column_options =  ['*'] + dbt.get_model_by_id(node_selected_name).get_columns().tolist()
        # this passing implies the model does exist
    except: # model does exists
        st.info('Model does not exist, this implies first run and we might not have all the columns ready')
        column_options =  ['*']
    
    select_columns_default = settings.get('select_columns', '*')
    select_columns = st.multiselect("Select columns",options = column_options, default=select_columns_default)
    settings['select_columns'] = select_columns
    return settings


@generic_input_wrap
def filter_input( *, settings, dbt, node_selected_name): # settings passed by wrapping function
    filter_where_clause_default = settings.get('filter_where_clause', '1')
    filter_where_clause = st.text_input("Where", value=filter_where_clause_default)
    settings['filter_where_clause'] = filter_where_clause
    return settings

def add_user_config_to_nodes(nodes, user_config):
    """Runs once before user enters settings (to hydrate columns) and then again after user enters settings"""
    new_nodes = []
    for node in nodes:
        node_config = user_config.get(node.name, {})
        node.upstream = node_config.get('upstream', node.upstream)
        node.settings = node_config.get('settings', {})
        new_nodes.append(node)
    return new_nodes

import os
import streamlit as st
import node_graph
from idbt.idbt import DbtProject, Node, DATA_SOURCE_NAMES, TEMPLATE_NAMES, SelectNode, FilterNode, AppendNode, MergeNode
DEBUG = True
st.session_state['node_user_config'] = st.session_state.get('node_user_config', {})

# define items the user can use
item_types = [
        {"title":'Data source',"color":"rgb(255,0, 192)", "port_selection" : 'out'},
        {"title":'Append',"color":"rgb(0,255, 192)", "port_selection" : 'both'},
        {"title":'Select',"color":"rgb(0,255, 192)", "port_selection" : 'both'},
        {"title":'Merge',"color":"rgb(0,255, 192)", "port_selection" : 'both'},
        {"title":'Filter',"color":"rgb(0,255, 192)", "port_selection" : 'both'},

    ]

TYPE_TO_NODE_MAPPING = {
    'merge': MergeNode,
    'append': AppendNode,
    'select': SelectNode,
    'filter': FilterNode,
}

diagram = node_graph.node_graph(item_types=item_types, key='test',)

def parse_nodes(json_data):
    if not json_data:
        return []
    # Extract nodes and links from the JSON data
    nodes_data = {}
    links_data = {}
    for layer in json_data['layers']:
        if layer['type'] == 'diagram-nodes':
            nodes_data = layer['models']
        elif layer['type'] == 'diagram-links':
            links_data = layer['models']

    # Build a map of source to target for links
    link_map = {}
    for link_id, link in links_data.items():
        source = link['source']
        target = link['target']
        if target not in link_map:
            link_map[target] = []
        upstream_name = nodes_data[source]['name']
        link_map[target].append(upstream_name)

    # Create Node objects
    nodes = []
    for node_id, node_info in nodes_data.items():
        node_name = node_info['name']
        node_type = node_info['type'].lower()
        if node_type == 'data source':
            # a data source is just selecting from a seed
            node_type = 'select'
        upstream_nodes = link_map.get(node_id, [])
        node_class_to_use = TYPE_TO_NODE_MAPPING.get(node_type, Node)
        new_node = node_class_to_use(name=node_name, type=node_type, upstream=upstream_nodes, simple_settings={})
        nodes.append(new_node)
    return nodes


nodes = parse_nodes(diagram.model)

if diagram.selected:
    if diagram.selected['type'] == 'Data source':
        data_source_file = st.selectbox("Select a data source", DATA_SOURCE_NAMES)
        data_source_name = os.path.splitext(data_source_file)[0]
        st.session_state['node_user_config'][diagram.selected['name']] = {'upstream': [data_source_name]}
    if diagram.selected['type'] == 'Merge':
        merge_col = st.text_input("Select a column to merge on", DATA_SOURCE_NAMES)
        st.session_state['node_user_config'][diagram.selected['name']] = {'settings': {'merge_column':merge_col }}


model_selected = diagram.selected.get('name')
nodes = add_user_config_to_nodes(nodes, st.session_state['node_user_config'])
dbt = DbtProject(user_inputted_nodes=nodes)

if st.sidebar.button("Rerun"):
    dbt = DbtProject(user_inputted_nodes=nodes)
    dbt.compile_models()
    dbt.run_project()

if st.sidebar.button('Full clean'):
    dbt.full_clean()
    # st.session_state['node_user_config'] = {} # to refresh state lets just refresh the page
if st.sidebar.button('Import seeds'):
    dbt.run_seed()

if diagram.selected:
    st.write(f"Selected node: {diagram.selected['name']}")
    st.write(f"Selected node type: {diagram.selected['type']}")
    if diagram.selected['type'] == 'Select':
        select_input(dbt=dbt, node_selected_name=diagram.selected['name'])
    if diagram.selected['type'] == 'Filter':
        filter_input(dbt=dbt, node_selected_name=diagram.selected['name'])
if st.button('Run dbt'):
    st.sidebar.write(st.session_state['node_user_config'])
    nodes = add_user_config_to_nodes(nodes, st.session_state['node_user_config'])
    dbt = DbtProject(user_inputted_nodes=nodes)
    dbt.compile_models()
    # dbt.run_project(select=f'"+{diagram.selected["name"]}+"')
    dbt.run_project(select=f'+{diagram.selected["name"]}+')
    if diagram.selected:
        model = dbt.get_model_by_id(diagram.selected['name'])
        st.write(model.get_df(method='df'))

