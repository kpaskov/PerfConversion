'''
Created on Jul 24, 2013

@author: kpaskov
'''

from perf_conversion import config as new_config, execute_conversion, \
    cache_by_key_in_range, get_json, cache_by_id_in_range, \
    create_or_update_and_remove, prepare_schema_connection, convert_f
from perf_conversion.link_maker import interaction_overview_table_link, \
    interaction_evidence_table_link, interaction_graph_link, \
    interaction_evidence_resource_link
from perf_conversion.output_manager import write_to_output_file
import json
import model_perf_schema

'''
 This code is used to convert interaction data from the new schema to the performance schema. 
 It does this by querying another backend (SGDBackend) for json over all possible queries and
 then storing that json in the performance database.
'''

def convert(new_session_maker7, ask=True):
    
    from model_perf_schema.interaction import BioentInteractionOverview, BioentInteractionEvidence, \
                            BioentInteractionGraph, BioentInteractionEvidenceResource
    
    intervals = [0, 500, 1000, 1500, 2000, 3000, 4000, 5000, 6000, 7000, 8000]
    # Convert interaction overview
    write_to_output_file('Interaction Overview')
    for i in range(0, len(intervals)-1):
        min_id = intervals[i]
        max_id = intervals[i+1]
        write_to_output_file('Bioent ids between ' + str(min_id) + ' and ' + str(max_id))
        cf = convert_f(BioentInteractionOverview, interaction_overview_table_link)
        execute_conversion(cf, new_session_maker, ask,
                       min_id = min_id,
                       max_id = max_id)
        
    # Convert interaction evidence
    write_to_output_file('Interaction Evidence')
    for i in range(0, len(intervals)-1):
        min_id = intervals[i]
        max_id = intervals[i+1]
        write_to_output_file('Bioent ids between ' + str(min_id) + ' and ' + str(max_id))
        cf = convert_f(BioentInteractionEvidence, interaction_evidence_table_link)
        execute_conversion(cf, new_session_maker, ask,
                       min_id = min_id,
                       max_id = max_id)
        
    # Convert interaction graph
    write_to_output_file('Interaction Graph')
    for i in range(0, len(intervals)-1):
        min_id = intervals[i]
        max_id = intervals[i+1]
        write_to_output_file('Bioent ids between ' + str(min_id) + ' and ' + str(max_id))
        cf = convert_f(BioentInteractionGraph, interaction_graph_link)
        execute_conversion(cf, new_session_maker, ask,
                       min_id = min_id,
                       max_id = max_id)
        
    # Convert interaction evidence resources
    write_to_output_file('Interaction Evidence Resources')
    for i in range(0, len(intervals)-1):
        min_id = intervals[i]
        max_id = intervals[i+1]
        write_to_output_file('Bioent ids between ' + str(min_id) + ' and ' + str(max_id))
        cf = convert_f(BioentInteractionEvidenceResource, interaction_evidence_resource_link)
        execute_conversion(cf, new_session_maker, ask,
                       min_id = min_id,
                       max_id = max_id)

    

if __name__ == "__main__":
    new_session_maker = prepare_schema_connection(model_perf_schema, new_config)
    convert(new_session_maker, False)
   
    
