'''
Created on Aug 22, 2013

@author: kpaskov
'''

from perf_conversion import config as new_config, execute_conversion, \
    prepare_schema_connection, convert_f
from perf_conversion.link_maker import go_references_link
from perf_conversion.output_manager import write_to_output_file
import model_perf_schema

def convert(new_session_maker7, ask=True):
    
    from model_perf_schema.go import GoReferences
    
    intervals = [0, 500, 1000, 1500, 2000, 3000, 4000, 5000, 6000, 7000, 8000]
    
    # Convert go references
    write_to_output_file('Go References')
    for i in range(0, len(intervals)-1):
        min_id = intervals[i]
        max_id = intervals[i+1]
        write_to_output_file('Bioent ids between ' + str(min_id) + ' and ' + str(max_id))
        cf = convert_f(GoReferences, go_references_link)
        execute_conversion(cf, new_session_maker, ask,
                       min_id = min_id,
                       max_id = max_id)
    

if __name__ == "__main__":
    new_session_maker = prepare_schema_connection(model_perf_schema, new_config)
    convert(new_session_maker, False)
