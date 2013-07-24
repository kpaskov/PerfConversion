'''
Created on Mar 6, 2013

@author: kpaskov
'''
from perf_conversion.config import backend_url

backend_start = backend_url
frontend_start = ''

def add_format_name_params(link, key_to_format_name):
    params = {}
    for key, format_name in key_to_format_name.iteritems():
        if format_name is not None:
            params[key] = format_name
        
    full_link = backend_start + link + '&'.join([key + '=' + value for key, value in params.iteritems()]) + '&callback=?'
    return full_link

#Bioentity Links
def all_bioentity_link():
    return add_format_name_params('/all_bioents', {})

#Interaction Links
def interaction_overview_table_link(bioent_key=None, reference_key=None):
    return add_format_name_params('/interaction_overview_table?', True, {'bioent':bioent_key, 'reference':reference_key})
def interaction_evidence_table_link(bioent_key=None, biorel_key=None):
    return add_format_name_params('/interaction_evidence_table?', True, {'bioent':bioent_key, 'biorel': biorel_key})
def interaction_graph_link(bioent_key=None):
    return add_format_name_params('/interaction_graph?', True, {'bioent':bioent_key})
def interaction_evidence_resource_link(bioent_key=None):
    return add_format_name_params('/interaction_evidence_resources?', True, {'bioent':bioent_key})
