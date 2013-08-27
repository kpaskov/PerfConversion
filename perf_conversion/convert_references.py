'''
Created on Jul 24, 2013

@author: kpaskov
'''
from perf_conversion import config as new_config, execute_conversion, \
    cache_by_key_in_range, get_json, create_or_update_and_remove, \
    prepare_schema_connection, convert_f
from perf_conversion.link_maker import all_reference_link
from perf_conversion.output_manager import write_to_output_file
import json
import model_perf_schema

def convert(new_session_maker, ask=True):
    
    from model_perf_schema.reference import ReferenceBib
    
    intervals = [0, 10000, 20000, 35000, 50000, 60000, 70000, 80000, 100000]
    # Convert references
    write_to_output_file('Reference')
    for i in range(0, len(intervals)-1):
        min_id = intervals[i]
        max_id = intervals[i+1]
        write_to_output_file('Reference ids between ' + str(min_id) + ' and ' + str(max_id))
        execute_conversion(convert_reference, new_session_maker, ask,
                       min_id = min_id,
                       max_id = max_id)
    
    # Convert ref_bibs references
    write_to_output_file('Phenotype References')
    for i in range(0, len(intervals) - 1):
        min_id = intervals[i]
        max_id = intervals[i + 1]
        write_to_output_file('Bioent ids between ' + str(min_id) + ' and ' + str(max_id))
        cf = convert_f(ReferenceBib, reference_bib_link)
        execute_conversion(cf, new_session_maker, ask,
                       min_id=min_id,
                       max_id=max_id)

    
def convert_reference(new_session, min_id=None, max_id=None):
    '''
    Convert Reference
    '''
    from model_perf_schema.reference import Reference
    
    #Cache references
    key_to_reference = cache_by_key_in_range(Reference, Reference.id, new_session, min_id, max_id)

    #Grab references from backend
    new_references = []
    references_json = get_json(all_reference_link(str(min_id), str(max_id)))
    for reference_json in references_json:
        new_references.append(Reference(reference_json['id'], json.dumps(reference_json)))
   
    success = create_or_update_and_remove(new_references, key_to_reference, ['json'], new_session)
    return success



if __name__ == "__main__":
    new_session_maker = prepare_schema_connection(model_perf_schema, new_config)
    convert(new_session_maker, False)
