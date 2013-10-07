'''
Created on Sep 25, 2013

@author: kpaskov
'''
from mpmath import ceil
from perf_conversion import get_json, create_or_update, set_up_logging, \
    prepare_schema_connection, config
from perf_conversion.link_maker import all_bioentity_link, all_reference_link, \
    interaction_overview_link, interaction_details_link, interaction_graph_link, \
    interaction_resources_link, interaction_references_link, \
    literature_overview_link, literature_details_link, literature_graph_link, \
    regulation_overview_link, regulation_details_link, regulation_references_link, \
    phenotype_references_link, go_references_link, binding_site_details_link, \
    protein_domain_details_link, all_bibentry_link, regulation_graph_link, \
    bioentitytabs_link
from perf_conversion.output_manager import OutputCreator
from threading import Thread
import json
import logging
import model_perf_schema
import sys

def convert_bioentity(sessionmaker, link, cls, chunk_size):
    log = logging.getLogger('convert.performance.' + cls.__name__)
    log.info('begin')
    output_creator = OutputCreator(log)
    
    try:
        session = sessionmaker()
        
        #Cache current objs
        current_objs = session.query(cls).all()
        id_to_current_obj = dict([(x.id, x) for x in current_objs])
        key_to_current_obj = dict([(x.unique_key(), x) for x in current_objs])
        
        untouched_obj_ids = set(id_to_current_obj.keys())
        
        #Grab new objs from backend
        objs_json = get_json(link())
        
        min_id = 0
        count = len(objs_json)
        num_chunks = ceil(1.0*count/chunk_size)
        for i in range(0, num_chunks):
            old_objs = objs_json[min_id:min_id+chunk_size]
            for obj_json in old_objs:
                newly_created_obj = cls(obj_json['id'], obj_json['format_name'], obj_json['display_name'], obj_json['bioent_type'], json.dumps(obj_json))
                current_obj_by_id = None if newly_created_obj.id not in id_to_current_obj else id_to_current_obj[newly_created_obj.id]
                current_obj_by_key = None if newly_created_obj.unique_key() not in key_to_current_obj else key_to_current_obj[newly_created_obj.unique_key()]
                create_or_update(newly_created_obj, current_obj_by_id, current_obj_by_key, ['format_name', 'class_type', 'json'], session, output_creator)
                                
                if current_obj_by_id is not None and current_obj_by_id.id in untouched_obj_ids:
                    untouched_obj_ids.remove(current_obj_by_id.id)
                if current_obj_by_key is not None and current_obj_by_key.id in untouched_obj_ids:
                    untouched_obj_ids.remove(current_obj_by_key.id)
                    
            #Commit
            output_creator.finished(str(i+1) + "/" + str(int(num_chunks)))
            session.commit()
            min_id = min_id+chunk_size
                
        #Delete untouched objs
        for untouched_obj_id  in untouched_obj_ids:
            session.delete(id_to_current_obj[untouched_obj_id])
            output_creator.removed()
        
        #Commit
        output_creator.finished()
        session.commit()
            
    except Exception:
        log.exception('Unexpected error:' + str(sys.exc_info()[0]))
    finally:
        session.close()
        
    log.info('complete')
    
def convert_bibentry(sessionmaker, link, cls, chunk_size):
    log = logging.getLogger('convert.performance.' + cls.__name__)
    log.info('begin')
    output_creator = OutputCreator(log)
    
    try:
        session = sessionmaker()
        
        #Cache current objs
        current_objs = session.query(cls).all()
        id_to_current_obj = dict([(x.id, x) for x in current_objs])
        
        untouched_obj_ids = set(id_to_current_obj.keys())
        
        #Grab new objs from backend
        objs_json = get_json(link())
        
        min_id = 0
        count = len(objs_json)
        num_chunks = ceil(1.0*count/chunk_size)
        for i in range(0, num_chunks):
            old_objs = objs_json[min_id:min_id+chunk_size]
            for obj_json in old_objs:
                newly_created_obj = cls(obj_json['id'], obj_json['text'])
                current_obj_by_id = None if newly_created_obj.id not in id_to_current_obj else id_to_current_obj[newly_created_obj.id]
                create_or_update(newly_created_obj, current_obj_by_id, current_obj_by_id, ['json'], session, output_creator)
                                
                if current_obj_by_id is not None and current_obj_by_id.id in untouched_obj_ids:
                    untouched_obj_ids.remove(current_obj_by_id.id)
                    
            #Commit
            output_creator.finished(str(i+1) + "/" + str(int(num_chunks)))
            session.commit()
            min_id = min_id+chunk_size
                
        #Delete untouched objs
        for untouched_obj_id  in untouched_obj_ids:
            session.delete(id_to_current_obj[untouched_obj_id])
            output_creator.removed()
        
        #Commit
        output_creator.finished()
        session.commit()
            
    except Exception:
        log.exception('Unexpected error:' + str(sys.exc_info()[0]))
    finally:
        session.close()
        
    log.info('complete')

def convert_core(sessionmaker, link, cls, chunk_size):
    log = logging.getLogger('convert.performance.' + cls.__name__)
    log.info('begin')
    output_creator = OutputCreator(log)
    
    try:
        session = sessionmaker()
        
        #Cache current objs
        current_objs = session.query(cls).all()
        id_to_current_obj = dict([(x.id, x) for x in current_objs])
        
        untouched_obj_ids = set(id_to_current_obj.keys())
        
        #Grab new objs from backend
        objs_json = get_json(link())
        
        min_id = 0
        count = len(objs_json)
        num_chunks = ceil(1.0*count/chunk_size)
        for i in range(0, num_chunks):
            old_objs = objs_json[min_id:min_id+chunk_size]
            for obj_json in old_objs:
                newly_created_obj = cls(obj_json['id'], json.dumps(obj_json))
                current_obj_by_id = None if newly_created_obj.id not in id_to_current_obj else id_to_current_obj[newly_created_obj.id]
                create_or_update(newly_created_obj, current_obj_by_id, current_obj_by_id, ['json'], session, output_creator)
                                
                if current_obj_by_id is not None and current_obj_by_id.id in untouched_obj_ids:
                    untouched_obj_ids.remove(current_obj_by_id.id)
                    
            #Commit
            output_creator.finished(str(i+1) + "/" + str(int(num_chunks)))
            session.commit()
            min_id = min_id+chunk_size
                
        #Delete untouched objs
        for untouched_obj_id  in untouched_obj_ids:
            session.delete(id_to_current_obj[untouched_obj_id])
            output_creator.removed()
        
        #Commit
        output_creator.finished()
        session.commit()
            
    except Exception:
        log.exception('Unexpected error:' + str(sys.exc_info()[0]))
    finally:
        session.close()
        
    log.info('complete')
    
def convert_by_bioentity(sessionmaker, link, cls, bioents):    
    log = logging.getLogger('convert.performance.' + cls.__name__)
    log.info('begin')
    output_creator = OutputCreator(log)
    
    try:
        session = sessionmaker()
         
        #Cache current objs
        current_objs = session.query(cls).all()
        id_to_current_obj = dict([(x.id, x) for x in current_objs])
        
        untouched_obj_ids = set(id_to_current_obj.keys())
        
        #Grab new objs from backend
        i = 0
        for bioent in bioents:
            json_obj = get_json(link(bioent.format_name, bioent.class_type))
            newly_created_obj = cls(bioent.id, json.dumps(json_obj))
            current_obj_by_id = None if newly_created_obj.id not in id_to_current_obj else id_to_current_obj[newly_created_obj.id]
            create_or_update(newly_created_obj, current_obj_by_id, current_obj_by_id, ['json'], session, output_creator)
                            
            if current_obj_by_id is not None and current_obj_by_id.id in untouched_obj_ids:
                untouched_obj_ids.remove(current_obj_by_id.id)

            i = i+1
            if i%1000 == 0:
                output_creator.finished(str(i))
                session.commit()
                
        #Delete untouched objs
        for untouched_obj_id  in untouched_obj_ids:
            session.delete(id_to_current_obj[untouched_obj_id])
            output_creator.removed()
        
        #Commit
            output_creator.finished()
            session.commit()
            
    except Exception:
        log.exception('Unexpected error:' + str(sys.exc_info()[0]))
    finally:
        session.close()
        
    log.info('complete')


  
"""
---------------------Convert------------------------------
"""  

def convert(session_maker):
    log = set_up_logging('convert.performance')
    log.info('begin')
        
    ################# Core Converts ###########################
    #Bioentity
    from model_perf_schema.bioentity import Bioentity
    #convert_bioentity(session_maker, all_bioentity_link, Bioentity, 1000)
    
    #Reference
    from model_perf_schema.reference import Reference
    convert_core(session_maker, all_reference_link, Reference, 2000)
    
    #Get bioents
    session = session_maker()
    bioents = session.query(Bioentity).all()
    session.close()

    ################# Converts in parallel ###########################
#    
#    #Bibentry
#    from model_perf_schema.reference import Bibentry    
#    class ConvertBibentryThread (Thread):
#        def run(self):
#            try:
#                convert_bibentry(session_maker, all_bibentry_link, Bibentry, 2000)
#            except Exception:
#                log.exception( "Unexpected error:" + str(sys.exc_info()[0]) )
#    ConvertBibentryThread().start()
#    
#    #Bioentitytabs
#    from model_perf_schema.bioentity import Bioentitytabs
#    class ConvertBioentitytabsThread (Thread):
#        def run(self):
#            try:
#                convert_by_bioentity(session_maker, bioentitytabs_link, Bioentitytabs, bioents)
#            except Exception:
#                log.exception( "Unexpected error:" + str(sys.exc_info()[0]) )
#    ConvertBioentitytabsThread().start()
#    
#    #Interaction section
#    from model_perf_schema.interaction import InteractionOverview, InteractionDetails, InteractionGraph, InteractionResources, InteractionReferences
#    class ConvertInteractionSectionThread (Thread):
#        def run(self):
#            try:
#                convert_by_bioentity(session_maker, interaction_overview_link, InteractionOverview, bioents)
#                convert_by_bioentity(session_maker, interaction_details_link, InteractionDetails, bioents)
#                convert_by_bioentity(session_maker, interaction_graph_link, InteractionGraph, bioents)
#                convert_by_bioentity(session_maker, interaction_resources_link, InteractionResources, bioents)
#                convert_by_bioentity(session_maker, interaction_references_link, InteractionReferences, bioents)
#            except Exception:
#                log.exception( "Unexpected error:" + str(sys.exc_info()[0]) )
#    ConvertInteractionSectionThread().start()
#    
#    #Literature section
#    from model_perf_schema.literature import LiteratureOverview, LiteratureDetails, LiteratureGraph
#    class ConvertLiteratureSectionThread (Thread):
#        def run(self):
#            try:
#                convert_by_bioentity(session_maker, literature_overview_link, LiteratureOverview, bioents)
#                convert_by_bioentity(session_maker, literature_details_link, LiteratureDetails, bioents)
#                convert_by_bioentity(session_maker, literature_graph_link, LiteratureGraph, bioents)
#            except Exception:
#                log.exception( "Unexpected error:" + str(sys.exc_info()[0]) )
#    ConvertLiteratureSectionThread().start()
#    
#    #Regulation section
#    from model_perf_schema.regulation import RegulationOverview, RegulationDetails, RegulationGraph, RegulationReferences
#    class ConvertRegulationSectionThread (Thread):
#        def run(self):
#            try:
#                convert_by_bioentity(session_maker, regulation_overview_link, RegulationOverview, bioents)
#                convert_by_bioentity(session_maker, regulation_details_link, RegulationDetails, bioents)
#                convert_by_bioentity(session_maker, regulation_graph_link, RegulationGraph, bioents)
#                convert_by_bioentity(session_maker, regulation_references_link, RegulationReferences, bioents)
#            except Exception:
#                log.exception( "Unexpected error:" + str(sys.exc_info()[0]) )
#    ConvertRegulationSectionThread().start()
#    
#    #Phenotype section
#    from model_perf_schema.phenotype import PhenotypeReferences
#    class ConvertPhenotypeSectionThread (Thread):
#        def run(self):
#            try:
#                convert_by_bioentity(session_maker, phenotype_references_link, PhenotypeReferences, bioents)
#            except Exception:
#                log.exception( "Unexpected error:" + str(sys.exc_info()[0]) )
#    ConvertPhenotypeSectionThread().start()
#    
#    #Go section
#    from model_perf_schema.go import GoReferences
#    class ConvertGoSectionThread (Thread):
#        def run(self):
#            try:
#                convert_by_bioentity(session_maker, go_references_link, GoReferences, bioents)
#            except Exception:
#                log.exception( "Unexpected error:" + str(sys.exc_info()[0]) )
#    ConvertGoSectionThread().start()
    
    #Protein section
    from model_perf_schema.protein import ProteinDomainDetails
    class ConvertProteinSectionThread (Thread):
        def run(self):
            try:
                convert_by_bioentity(session_maker, protein_domain_details_link, ProteinDomainDetails, bioents)
            except Exception:
                log.exception( "Unexpected error:" + str(sys.exc_info()[0]) )
    ConvertProteinSectionThread().start()
    
    #Misc
    from model_perf_schema.misc import BindingSiteDetails
    class ConvertMiscThread (Thread):
        def run(self):
            try:
                convert_by_bioentity(session_maker, binding_site_details_link, BindingSiteDetails, bioents)
            except Exception:
                log.exception( "Unexpected error:" + str(sys.exc_info()[0]) )
    ConvertMiscThread().start()

if __name__ == "__main__":
    session_maker = prepare_schema_connection(model_perf_schema, config)
    convert(session_maker)
    
    