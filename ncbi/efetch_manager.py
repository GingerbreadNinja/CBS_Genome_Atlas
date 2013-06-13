
import heapq
import Bio.Entrez
import copy
import time
import math
import tempfile
import socket

class EFetchManager():
    '''
    This class is responsible for queueing and managing requests to
    the NCBI-Entrez efetch service.
    '''
    
    def __init__(self, logger, settings):
        self.__logger = logger
        try:
            Bio.Entrez.email = settings['email'] #Email is required
        except KeyError:
            logger.error('An email address is required for sending Entrez requests')
            raise KeyError('EFetch settings dictionary requires key \'email\' in order to operate')
        self.__max_attempts = settings.get('max_attempts', 10)
        self.__max_lookup = settings.get('max_lookup', 50)
        timeout = settings.get('timeout', None)
        
        if(timeout):
            logger.info('Setting default timeout on socket to %i', timeout)
            socket.setdefaulttimeout(timeout)
        if(not socket.getdefaulttimeout()):
            logger.warn('No socket timeout set! Lack of timeout could hang execution!')
        
    def __send_request(self, handler, uid_list):
        '''
        Builds the request using the input uid list. Figures the database and
        the request dictionary from the handler. Sends the request and offloads
        to the handler.
        '''
        self.__logger.info('Fetching %i UID\'s in NCBI-Entrez', len(uid_list))
        uid_str_list = map(str, uid_list)
        self.__logger.debug('Bioproject Request List: %s', uid_str_list)
        request_dict = copy.deepcopy(handler.get_request_dict())
        request_dict['id'] = uid_str_list
        
        db = handler.get_db()
        if not db or not isinstance(db, str):
            raise TypeError('Expected (non-empty) string for handler\'s db')
        
        output = tempfile.TemporaryFile()
        handle = None
        try:
            handle = Bio.Entrez.efetch(db,**request_dict)
            output.write(handle.read())
        except Exception, e:
            self.__logger.warn('A network error occurred downloading UID\'s')
            self.__logger.debug( e.message )
            if(output):
                output.close()
        finally:
            if(handle):
                self.__logger.info('Closing Entrez Socket')
                handle.close()
        try:
            output.seek(0)
            handler.handle(output)
        except Exception, e:
            self.__logger.warn('Error occurred during parse')
            self.__logger.debug(e)
        finally:
            output.close()
        # Biopython does this automatically
        # self.__logger.info('Waiting a couple seconds so as not to upset NCBI...')
        # time.sleep(3)
            
    def fetch_uids(self, handler, uid_list):
        '''
        Fetches the UID list in groups of max_lookups at a time. Will stop
        trying to find a specific UID if it fails more than max_attempts
        times. Builds a priority queue for processing, so that lookups which
        haven't run yet/have failed less get run first.
        '''
        
        self.__logger.info('Looking up Entrez-UID\'s in chunks')
        uid_set = set(uid_list)
        self.__logger.info('Reduced from %i to %i unique identifiers', len(uid_list), len(uid_set))
        pd = dict((bio, 0) for bio in uid_set)
        pq = [(p, bio) for bio, p in pd.items()]
        del uid_set
        heapq.heapify(pq) #Build the priority queue
        
        full_results = {}
        errors = []
        
        old_max = self.__max_lookup
        
        '''
        While there are things left to process...
           Build the lookup list by:
               adding items until we are out of items to add
               or until we cannot fit any more in the request
           Send the request for the lookup list
           Process the results so that successful lookups are stored in a uid->result hash table
           If failed item is not past max_attempts:
               retry failed item
           Else
               add failed item to error list
        '''
        while pq:
            self.__logger.info('%i UID\'s remaining download', len(pq))
            uid_lookup_list = []
            while pq and len(uid_lookup_list) < self.__max_lookup:
                p, uid = heapq.heappop(pq)
                uid_lookup_list.append(uid)
            
            handler.clear_results()
            self.__send_request(handler, uid_lookup_list)
            results = handler.get_results()
            full_results.update(results)
            self.__logger.info('Successfully looked up %i UID\'s', len(results))
            if(not results):
                self.__logger.warn('Error downloading any UID\'s')
                new_max = math.ceil( self.__max_lookup/2 )
                self.__logger.info('Cutting down from %i lookups to %i', self.__max_lookup, new_max)
                self.__max_lookup = new_max
            elif(old_max != self.__max_lookup):
                self.__logger.info('Successfully downloaded UID\'s after failures')
                new_max = min( self.__max_lookup + 2, old_max)
                self.__logger.info('Increasing from %i lookups to %i', self.__max_lookup, new_max)
                self.__max_lookup = new_max
                
            for uid in uid_lookup_list:
                if not results.has_key(uid):
                    pd[uid] = pd[uid] + 1
                    if pd[uid] < self.__max_attempts:
                        heapq.heappush(pq, (pd[uid], uid))
                    else:
                        self.__logger.error('Lookup of uid %i in db=%s has exceeded maximum attempts (%i), will ignore taxonomy lookup of item', uid, handler.get_db(), self.__max_attempts)
                        errors.append(uid)
        self.__max_lookup = old_max
        return full_results, errors
        
class EFetchHandler():
    
    __results = {}
    
    def clear_results(self):
        self.__results = {}
    
    def get_results(self):
        return self.__results
    
    def append_result(self, uid, result):
        self.__results[uid]=result
    
    def get_request_dict(self):
        return {}
    
    def get_db(self):
        return ''
    
    def handle(self, request_handle):
        pass
