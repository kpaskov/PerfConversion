'''
Created on Jul 24, 2013

@author: kpaskov
'''
from model_perf_schema import config as new_config
from perf_conversion import execute_conversion, \
    cache_by_key_in_range, get_json, create_or_update_and_remove, \
    prepare_schema_connection
from perf_conversion.link_maker import all_bioentity_link
from perf_conversion.output_manager import write_to_output_file
import model_perf_schema

def convert(new_session_maker, ask=True):
    
    intervals = [0, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000]
    # Convert bioentities
    write_to_output_file('Bioentity')
    for i in range(0, len(intervals)-1):
        min_id = intervals[i]
        max_id = intervals[i+1]
        write_to_output_file('Bioent ids between ' + str(min_id) + ' and ' + str(max_id))
        execute_conversion(convert_bioentity, new_session_maker, ask,
                       min_id = min_id,
                       max_id = max_id)

    
def convert_bioentity(new_session, min_id=None, max_id=None):
    '''
    Convert Bioentity
    '''
    from model_perf_schema.bioentity import BioentMap
    
    #Cache bioentities
    key_to_bioents = cache_by_key_in_range(BioentMap, BioentMap.bioent_id, new_session, min_id, max_id)

    #Grab bioentities from backend
    new_bioentities = []
    bioents_json = get_json(all_bioentity_link())
    for bioent_json in bioents_json:
        new_bioentities.append(BioentMap(bioent_json['format_name'], bioent_json['bioent_type'], bioent_json['bioent_id']))
   
    success = create_or_update_and_remove(new_bioentities, key_to_bioents, ['bioent_id'], new_session)
    return success

if __name__ == "__main__":
    new_session_maker = prepare_schema_connection(model_perf_schema, new_config)
    convert(new_session_maker, False)