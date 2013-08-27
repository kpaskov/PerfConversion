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
        
    full_link = backend_start + link + '&'.join([key + '=' + value for key, value in params.iteritems()])
    return full_link

#Bioentity Links
def all_bioentity_link(min_id, max_id):
    return add_format_name_params('/all_bioents?', {'min':min_id, 'max':max_id}) + '&callback=?'

#Reference Links
def all_reference_link(min_id, max_id):
    return add_format_name_params('/all_references?', {'min':min_id, 'max':max_id}) + '&callback=?'
def reference_list_link(bioent, bioent_type):
    return backend_start + '/' + bioent_type + '/' + str(bioent) + '/interaction_overview'

#Interaction Links
def interaction_overview_link(bioent, bioent_type):
    return backend_start + '/' + bioent_type + '/' + str(bioent) + '/interaction_overview'
def interaction_details_link(bioent, bioent_type):
    return backend_start + '/' + bioent_type + '/' + str(bioent) + '/interaction_details'
def interaction_graph_link(bioent, bioent_type):
    return backend_start + '/' + bioent_type + '/' + str(bioent) + '/interaction_graph'
def interaction_resources_link(bioent, bioent_type):
    return backend_start + '/' + bioent_type + '/' + str(bioent) + '/interaction_resources'
def interaction_references_link(bioent, bioent_type):
    return backend_start + '/' + bioent_type + '/' + str(bioent) + '/interaction_references'

#Literature Links
def literature_overview_link(bioent, bioent_type):
    return backend_start + '/' + bioent_type + '/' + str(bioent) + '/literature_overview'
def literature_details_link(bioent, bioent_type):
    return backend_start + '/' + bioent_type + '/' + str(bioent) + '/literature_details'
def literature_graph_link(bioent, bioent_type):
    return backend_start + '/' + bioent_type + '/' + str(bioent) + '/literature_graph'

#Go Links
def go_references_link(bioent, bioent_type):
    return backend_start + '/' + bioent_type + '/' + str(bioent) + '/go_references'

#Phenotype Links
def phenotype_references_link(bioent, bioent_type):
    return backend_start + '/' + bioent_type + '/' + str(bioent) + '/phenotype_references'
