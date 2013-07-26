'''
Created on Jul 24, 2013

@author: kpaskov
'''

from perf_conversion import config as new_config, execute_conversion, \
    cache_by_key_in_range, get_json, cache_by_id_in_range, \
    create_or_update_and_remove, prepare_schema_connection, convert_f
from perf_conversion.link_maker import interaction_overview_table_link, \
    interaction_evidence_table_link
from perf_conversion.output_manager import write_to_output_file
import model_perf_schema
import simplejson

'''
 This code is used to convert interaction data from the new schema to the performance schema. 
 It does this by querying another backend (SGDBackend) for json over all possible queries and
 then storing that json in the performance database.
'''

def convert(new_session_maker, ask=True):
    from model_perf_schema.interaction import BioentInteractionOverview
    
    intervals = [0, 500, 1000, 1500, 2000, 3000, 4000, 5000, 6000, 7000, 8000]
#    # Convert interaction overview
#    write_to_output_file('Interaction Overview')
#    for i in range(0, len(intervals)-1):
#        min_id = intervals[i]
#        max_id = intervals[i+1]
#        write_to_output_file('Bioent ids between ' + str(min_id) + ' and ' + str(max_id))
#        cf = convert_f(BioentInteractionOverview, interaction_overview_table_link)
#        execute_conversion(cf, new_session_maker, ask,
#                       min_id = min_id,
#                       max_id = max_id)
        
    # Convert interaction evidence
    write_to_output_file('Interaction Evidence')
    for i in range(0, len(intervals)-1):
        min_id = intervals[i]
        max_id = intervals[i+1]
        write_to_output_file('Bioent ids between ' + str(min_id) + ' and ' + str(max_id))
        execute_conversion(convert_interaction_evidence, new_session_maker, ask,
                       min_id = min_id,
                       max_id = max_id)
#        
#    # Convert interaction graph
#    write_to_output_file('Interaction Graph')
#    for i in range(0, len(intervals)-1):
#        min_id = intervals[i]
#        max_id = intervals[i+1]
#        write_to_output_file('Bioent ids between ' + str(min_id) + ' and ' + str(max_id))
#        execute_conversion(convert_interaction_graph, new_session_maker, ask,
#                       min_id = min_id,
#                       max_id = max_id)

    
def convert_interaction_overview(new_session, min_id=None, max_id=None):
    '''
    Convert Genetic overviews
    '''
    from model_perf_schema.interaction import BioentInteractionOverview
    from model_perf_schema.bioentity import BioentMap
    
    #Cache interaction overviews
    id_to_bioents = cache_by_id_in_range(BioentMap, BioentMap.bioent_id, new_session, min_id, max_id)
    key_to_overviews = cache_by_key_in_range(BioentInteractionOverview, BioentInteractionOverview.bioent_id, new_session, min_id, max_id)

    #Grab interaction overviews from backend
    new_overviews = []
    for bioent_id in range(min_id, max_id):
        if bioent_id in id_to_bioents:
            bioent_name = id_to_bioents[bioent_id].format_name
            json = simplejson.dumps(get_json(interaction_overview_table_link(bioent_key=bioent_name)))
            new_overviews.append(BioentInteractionOverview(bioent_id, json))
   
    success = create_or_update_and_remove(new_overviews, key_to_overviews, ['json'], new_session)
    return success

def convert_interaction_evidence(new_session, min_id=None, max_id=None):
    '''
    Convert Genetic Interevidences
    '''
    from model_perf_schema.interaction import BioentInteractionEvidence
    from model_perf_schema.bioentity import BioentMap
    
    #Cache interaction overviews
    id_to_bioents = cache_by_id_in_range(BioentMap, BioentMap.bioent_id, new_session, min_id, max_id)
    key_to_evidences = cache_by_key_in_range(BioentInteractionEvidence, BioentInteractionEvidence.bioent_id, new_session, min_id, max_id)

    #Grab interaction evidences from backend
    new_evidences = []
    for bioent_id in range(min_id, max_id):
        if bioent_id in id_to_bioents:
            bioent_name = id_to_bioents[bioent_id].format_name
            json = simplejson.dumps(get_json(interaction_evidence_table_link(bioent_key=bioent_name)))
            new_evidences.append(BioentInteractionEvidence(bioent_id, json))
   
    success = create_or_update_and_remove(new_evidences, key_to_evidences, ['json'], new_session)
    return success

if __name__ == "__main__":
    new_session_maker = prepare_schema_connection(model_perf_schema, new_config)
    convert(new_session_maker, False)
   
    
