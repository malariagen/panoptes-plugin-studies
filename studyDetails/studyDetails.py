from __future__ import unicode_literals
import ldap, ldap.sasl
import unicodecsv
import sys
from collections import OrderedDict
import urllib2
import os
import json
from responders.importer.BasePlugin import BasePlugin

    
class studyDetails(BasePlugin):
           
    def getSettings(self):
        settingsDef = OrderedDict((('plugin', {
                                               'type': 'Text',
                                               'required': True,
                                               'description': "The name of the plugin class to run",
                                               'values':  { 'studyDetails': { 'description' : 'The name of the plugin class'} }
                                               }),
                                   ('ldapServer', {
                                                 'type': 'Text',
                                                 'required': False,
                                                 'description': 'If this is none then no people information will be retrieved from LDAP and included in the output',
                                                 'default': None
                                                 }),
                                   ('userDN', {
                                                 'type': 'Text',
                                                 'description': 'The DN of the user to connect to LDAP - this user needs permission to read people records',
                                                 'required': False
                                                 }),
                                   ('ldapPassword', {
                                                 'type': 'Text',
                                                 'description': 'The LDAP password',
                                                 'required': False
                                                 }),
                                   ('userId', {
                                                 'type': 'Text',
                                                 'description': 'The id used to retrieve the study information from Alfresco',
                                                 'required': True
                                                 }),
                                   ('password', {
                                                 'type': 'Text',
                                                 'description': 'The password associated with userId'
                                                 }),
                                   ('project', {
                                                 'type': 'Text',
                                                 'description': 'The name of a project can be included to restrict which studies are loaded - should usually be set',
                                                 'required': False,
                                                 'default': None
                                                 }),
                                   ('studiesURL', {
                                                 'type': 'Text',
                                                 'description': 'Where to fetch the study descriptions - can usually be left as default',
                                                 'default': 'https://alfresco.malariagen.net/alfresco/service/cggh/collaborations'
                                                 }),
                                  ('dataset', {
                                                 'type': 'Text',
                                                 'description': 'The name of the dataset',
                                                 'required': True
                                                 }),
                                  ('studies_datatable', {
                                                 'type': 'Text',
                                                 'description': 'The name of the data table where the study records will be written',
                                                 'default': 'studies'
                                                 }),
                                  ('study_people_datatable', {
                                                 'type': 'Text',
                                                 'description': 'The name of the data table where the people records associated with each study will be written',
                                                 'default': 'study_people'
                                                 }),
                                  ('study_publications_datatable', {
                                                 'type': 'Text',
                                                 'description': 'The name of the data table where the publication records associated with each study will be written',
                                                 'default': 'study_publications'
                                                 }),
                                   ('study_people_fields', {
                                                 'type': 'List',
                                                 'description': 'Which people fields to include in the study_people_datatable, in the same order. The field names will need to match both LDAP and the datatable settings.',
                                                 'default': ["jobTitle1", "jobTitle2", "jobTitle3", "uid", "researchGateURL", "scholarURL", "twitterURL", "malariagenUID", "oProfile1", "oProfile2", "oProfile3", "ORCID", "sn", "mail", "givenName", "o1", "o2", "o3"]
                                                 }),
                                   ('study_publications_fields', {
                                                 'type': 'List',
                                                 'description': 'Which publication fields to include in the study_publications_datatable, in the same order. The field names will need to match both Alfresco and the datatable settings.',
                                                 'default': ["doi", "name", "title", "citation", "pmid"]
                                                 }),
                                   ('peopleTypes', {
                                                 'type': 'List',
                                                 'description': 'Which user categories to include in the output - will be done in order',
                                                 'default': ['Contact', 'Public']
                                                 })
                                   ))
        return settingsDef

        
    def run(self):
        
        # Determine the paths to the datatable directories.
        datatables_path = os.path.join(self._config.getSourceDataDir(), "datasets", self._plugin_settings["dataset"], "datatables")
        studies_datatable_path =  os.path.join(datatables_path, self._plugin_settings["studies_datatable"])
        study_people_datatable_path =  os.path.join(datatables_path, self._plugin_settings["study_people_datatable"])
        study_publications_datatable_path =  os.path.join(datatables_path, self._plugin_settings["study_publications_datatable"])
    
        # Create the datatable directories, if they don't already exist.
        if os.path.isdir(studies_datatable_path) != True:
            sys.stdout.write("Making the studies datatable directory, i.e. " + studies_datatable_path + '\n')
            os.makedirs(studies_datatable_path)
        if os.path.isdir(study_people_datatable_path) != True:
            sys.stdout.write("Making the study_people datatable directory, i.e. " + study_people_datatable_path + '\n')
            os.makedirs(study_people_datatable_path)
        if os.path.isdir(study_publications_datatable_path) != True:
            sys.stdout.write("Making the study_publications datatable directory, i.e. " + study_publications_datatable_path + '\n')
            os.makedirs(study_publications_datatable_path)
    
        # Only process studies that are associated with a particular project
        filter_by_project_name = self._plugin_settings["project"]
        
        # Specify the CSV file item separators.
        csv_value_separator = "\t"
        csv_row_separator = "\n"
        csv_list_separator = "; "
        
        # Determine the paths to the data files.
        studies_csv_file_path = os.path.join(studies_datatable_path, "data")
        study_people_csv_file_path = os.path.join(study_people_datatable_path, "data")
        study_publications_csv_file_path = os.path.join(study_publications_datatable_path, "data")
        
        # Print a warning if any of the data files already exist.
        if os.path.isfile(studies_csv_file_path) == True:
            print("Warning: Overwriting file: " + studies_csv_file_path)
        if os.path.isfile(study_people_csv_file_path) == True:
            print("Warning: Overwriting file: " + study_people_csv_file_path)
        if os.path.isfile(study_publications_csv_file_path) == True:
            print("Warning: Overwriting file: " + study_publications_csv_file_path)
        
        # Open the CSV files for writing.
        studies_csv_file = open(studies_csv_file_path, 'w')
        study_people_csv_file = open(study_people_csv_file_path, 'w')
        study_publications_csv_file = open(study_publications_csv_file_path, 'w')
        
        # Specify which fields to include and their order.
        study_people_fields = self._plugin_settings["study_people_fields"]
        study_publications_fields = self._plugin_settings["study_publications_fields"]
        
        # Append the heading lines.
        studies_csv_file.write(csv_value_separator.join(["Study_number", "webTitle", "description"]) + csv_row_separator)
        study_people_csv_file.write(csv_value_separator.join(["Study_number"] + study_people_fields) + csv_row_separator)
        study_publications_csv_file.write(csv_value_separator.join(["Study_number"] + study_publications_fields) + csv_row_separator)
        
        # Load the JSON into a Python object
        collaborations = self.fetchDetails()

        people = {}
        fields = ['mail',
                     'jobTitle1',
                     'givenName',
                     'sn',
                     'jobTitle1',
                     'o1',
                     'jobTitle2',
                     'o2',
                     'jobTitle3',
                     'o3',
                     'oProfile1',
                     'oProfile2',
                     'oProfile3',
                     'linkedInURL',
                     'twitterURL',
                     'researchGateURL',
                     'scholarURL',
                     'ORCID',
                     'malariagenUID',
                     'uid']
        self.list_ldap(people, fields)
        
        
        self._calculationObject.SetInfo('Creating studies data file')
        
        if len(collaborations["collaborationNodes"]) == 0:
            self._log("Warning: zero collaborationNodes")
        
        # Loop through the collaboration nodes (each represents a study)
        for study in collaborations["collaborationNodes"]:
            
            # Get the list of projects
            projects = study["projects"]
            
            # See if this study is in the project we want
            # by looking through the list of associated project names.
            is_in_project = False
            for project in projects:
                
                project_name = project["name"];
                
                if filter_by_project_name is None or project_name == filter_by_project_name:
                    is_in_project = True
                    break
            
            # If this study isn't in the project, then skip to the next study.
            if is_in_project != True:
                continue
            
            # Compose the study row, which will be appended to the CSV file
            # Study_number    webTitle    description
            
            Study_number = study['name'].split('-')[0].replace(csv_value_separator, "")
            
            study_row = []
            study_row.append(Study_number)
            if study["webTitleApproved"] == "false":
                self._log("Warning: webTitle not approved for:" + study['name'])
            study_row.append(study["webTitle"].replace(csv_value_separator, ""))
            
            if study["descriptionApproved"] == "false":
                self._log("Warning: description not approved for:" + study['name'])
            study_row.append(study["description"].replace(csv_value_separator, ""))
            
            study_people = self.study_people(people, study)
            
            for study_person in study_people:
                study_person_values = []
                study_person_values.append(Study_number)
                for study_person_field in study_people_fields:
                    study_person_field_value = ''
                    if study_person_field in study_person:
                        if isinstance(study_person[study_person_field], basestring):
                            study_person_field_value = study_person[study_person_field]
                        elif isinstance(study_person[study_person_field], (list, tuple)):
                            study_person_field_value = csv_list_separator.join(study_person[study_person_field])
                    study_person_values.append(study_person_field_value)
                # Write the study_person_values to the CSV file
                study_people_csv_file.write(csv_value_separator.join(study_person_values).encode('ascii', 'xmlcharrefreplace').encode('latin-1').replace("\n", " ").replace("\r", " ") + csv_row_separator)

            for study_publication in study["publications"]:
                study_publication_values = []
                study_publication_values.append(Study_number)
                for study_publication_field in study_publications_fields:
                    study_publication_field_value = ''
                    if study_publication_field in study_publication:
                        if isinstance(study_publication[study_publication_field], basestring):
                            study_publication_field_value = study_publication[study_publication_field]
                        elif isinstance(study_publication[study_publication_field], (list, tuple)):
                            study_publication_field_value = csv_list_separator.join(study_publication[study_publication_field])
                    study_publication_values.append(study_publication_field_value)
                # Write the study_publication_values to the CSV file
                study_publications_csv_file.write(csv_value_separator.join(study_publication_values).encode('ascii', 'xmlcharrefreplace').encode('latin-1').replace("\n", " ").replace("\r", " ") + csv_row_separator)
                
            # Write the study to the CSV file
            studies_csv_file.write(csv_value_separator.join(study_row).encode('ascii', 'xmlcharrefreplace').encode('latin-1').replace("\n", " ").replace("\r", " ") + csv_row_separator)
        
        
        
        # Close the CSV file
        studies_csv_file.close()





    def fetchDetails(self):
        
        self._calculationObject.SetInfo('Fetching study details')
        # create a password manager
        password_mgr = urllib2.HTTPPasswordMgrWithDefaultRealm()

        top_level_url = self._plugin_settings["studiesURL"]
        password_mgr.add_password(None, top_level_url, self._plugin_settings["userId"], self._plugin_settings["password"])
        
        handler = urllib2.HTTPBasicAuthHandler(password_mgr)
        
        # create "opener" (OpenerDirector instance)
        opener = urllib2.build_opener(handler)
        
        json_url = self._plugin_settings["studiesURL"]
        # use the opener to fetch a URL
        
        json_file = opener.open(json_url)
        
        data = json.load(json_file)

        #print json.dumps(data, indent=2, sort_keys=True)

        json_file.close()
        
        return data
                
    def open_ldap(self):
        server = self._plugin_settings["ldapServer"]
        user_dn = self._plugin_settings['userDN']
        user_pw = self._plugin_settings['ldapPassword']

        if server == None:
            return
        
        l = ldap.initialize(server)
        try: 
            #l.start_tls_s()
            l.bind_s(user_dn, user_pw)
        except ldap.INVALID_CREDENTIALS:
            self._log("Your username or password is incorrect.")
            sys.exit()
        except ldap.LDAPError, e:
            if type(e.message) == dict and e.message.has_key('desc'):
                self._log(e.message['desc'])
            else: 
                self._log(str(e))
        return l
        auth_tokens = ldap.sasl.digest_md5( user_dn, user_pw )
        try:
          l.sasl_interactive_bind_s( "", auth_tokens )
        except ldap.INVALID_CREDENTIALS, e :
          self._log(str(e))
        return l
    
    def handle_ldap_entry(self, dn, entry, people, fields):
       
        if not 'malariagenUID' in entry:
            self._log(("No malariagenUID in ",dn, " ",str(entry)))
            return
        
        malariagenUID = unicode(entry['malariagenUID'][0],"utf-8")
            
        people[malariagenUID] = { 'dn': dn }
        
        for field in fields:
            if field in entry:
                people[malariagenUID][field] = unicode(entry[field][0],"utf-8")

    def list_ldap(self, people, fields):
        self._calculationObject.SetInfo('Fetching person details')
        l = self.open_ldap()
        if l == None:
            return
        r = l.search_s('ou=people,dc=malariagen,dc=net',ldap.SCOPE_SUBTREE,'(objectClass=OpenLDAPperson)',[str(x) for x in fields])
        for dn,entry in r:
    #      print 'Processing',repr(dn)
          self.handle_ldap_entry(dn, entry, people, fields)
        l.unbind()



    def study_people(self, people, study):
        
        study_people = {}
        study_list = []
        
        for group_type in self._plugin_settings["peopleTypes"]:
            group = study["group" + group_type]
            for study_person in group:
                malariagenUID = study_person['malariagenUID']
                if malariagenUID in people:
                    person = people[malariagenUID]
                    if malariagenUID in study_people:
                        for p in study_list:
                            if p['malariagenUID'] == malariagenUID:
                                p['class'].append(group_type)
                    else:
                        person['class'] = [group_type]
                        study_people[malariagenUID] = person
                        study_list.append(person)
                    
        return study_list
                
