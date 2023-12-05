import os
import streamlit as st
import node_graph
from idbt.idbt import DbtProject, Node, DATA_SOURCE_NAMES, TEMPLATE_NAMES

st.session_state['node_user_config'] = st.session_state.get('node_user_config', {})

# define items the user can use
item_types = [
        {"title":'Data source',"color":"rgb(255,0, 192)", "port_selection" : 'out'},
        {"title":'Append',"color":"rgb(0,255, 192)", "port_selection" : 'both'},
        {"title":'Select',"color":"rgb(0,255, 192)", "port_selection" : 'both'},
        {"title":'Merge',"color":"rgb(0,255, 192)", "port_selection" : 'both'},
    ]

diagram = node_graph.node_graph(item_types=item_types, key='test',)

if diagram.selected:
    if diagram.selected['type'] == 'Data source':
        data_source_file = st.selectbox("Select a data source", DATA_SOURCE_NAMES)
        data_source_name = os.path.splitext(data_source_file)[0]
        st.session_state['node_user_config'][diagram.selected['name']] = {'upstream': [data_source_name]}

def parse_nodes(json_data):
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
        new_node = Node(name=node_name, type=node_type, upstream=upstream_nodes, simple_settings={})
        nodes.append(new_node)

    return nodes

nodes = parse_nodes(diagram.model)

def add_user_config_to_nodes(nodes, user_config):
    for node in nodes:
        node_config = user_config.get(node.name, {})
        node.upstream = node_config.get('upstream', node.upstream)
        # node_config['settings'] = node_config.get('settings', node_config['settings'])
    return nodes
nodes = add_user_config_to_nodes(nodes, st.session_state['node_user_config'])
for node in nodes:
    st.write(node)

if st.sidebar.button('Run dbt'):
    dbt = DbtProject(user_inputted_nodes=nodes)

    dbt.run_seed()
    dbt.compile_models()
    dbt.run_project()
    st.write(dbt.dbt_models[0].get_df(method='df'))