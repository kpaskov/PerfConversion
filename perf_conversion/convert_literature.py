'''
Created on Jul 24, 2013

@author: kpaskov
'''

from perf_conversion import config as new_config, execute_conversion, \
    prepare_schema_connection, convert_f
from perf_conversion.link_maker import literature_graph_link, \
    literature_overview_link, literature_details_link
from perf_conversion.output_manager import write_to_output_file
import model_perf_schema

'''
 This code is used to convert interaction data from the new schema to the performance schema. 
 It does this by querying another backend (SGDBackend) for json over all possible queries and
 then storing that json in the performance database.
'''

def convert(new_session_maker, ask=True):
    
    from model_perf_schema.literature import LiteratureOverview, LiteratureDetails, LiteratureGraph
    
    intervals = [0, 500, 1000, 1500, 2000, 3000, 4000, 5000, 6000, 7000, 8000]
    # Convert literature overview
    write_to_output_file('Literature Overview')
    for i in range(0, len(intervals)-1):
        min_id = intervals[i]
        max_id = intervals[i+1]
        write_to_output_file('Bioent ids between ' + str(min_id) + ' and ' + str(max_id))
        cf = convert_f(LiteratureOverview, literature_overview_link)
        execute_conversion(cf, new_session_maker, ask,
                       min_id = min_id,
                       max_id = max_id)
        
    # Convert literature evidence
    write_to_output_file('Literature Details')
    for i in range(0, len(intervals)-1):
        min_id = intervals[i]
        max_id = intervals[i+1]
        write_to_output_file('Bioent ids between ' + str(min_id) + ' and ' + str(max_id))
        cf = convert_f(LiteratureDetails, literature_details_link)
        execute_conversion(cf, new_session_maker, ask,
                       min_id = min_id,
                       max_id = max_id)
        
    # Convert literature graph
    write_to_output_file('Literature Graph')
    for i in range(0, len(intervals)-1):
        min_id = intervals[i]
        max_id = intervals[i+1]
        write_to_output_file('Bioent ids between ' + str(min_id) + ' and ' + str(max_id))
        cf = convert_f(LiteratureGraph, literature_graph_link)
        execute_conversion(cf, new_session_maker, ask,
                       min_id = min_id,
                       max_id = max_id)
        

    

if __name__ == "__main__":
    new_session_maker = prepare_schema_connection(model_perf_schema, new_config)
    convert(new_session_maker, False)
   
    
