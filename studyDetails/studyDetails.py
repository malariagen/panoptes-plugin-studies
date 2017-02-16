from __future__ import unicode_literals
import ldap, ldap.sasl
import unicodecsv
import sys
from collections import OrderedDict
import urllib2
import os
import json
from jinja2 import Template
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
                                  ('datatable', {
                                                 'type': 'Text',
                                                 'description': 'The name of the data table where the output will be written',
                                                 'default': 'studies'
                                                 }),
                                   ('userTemplate', {
                                                 'type': 'Text',
                                                 'description': 'jinja2 template describing how the user information will be output',
                                                 'default': '''<ul class="people">{% for user in users %}
    <li class="person"><div class="name">{{user.givenName}} {{user.sn}}</div>
    <div class="company">{{user.o1}}</div>
    {% if 'Contact' in user.class %}
    <div class="email_container"><a href="mailto:{{user.mail}}"><span class="fa fa-envelope"></span><span class="email">{{user.mail}}</span></a></div>
    {% endif %}
    </li>
  {% endfor %}</ul>'''
                                                 }),
                                   ('publicationsTemplate', {
                                                 'type': 'Text',
                                                 'description': 'jinja2 template describing how the publication information will be output',
                                                 'default': '''
<ul class="publications">
            
    {% for publication in publications %}
        <li class="publication">
            <span class="citation">{{publication.citation}}</span>
            <dl>
                <dt>DOI</dt><dd><a href="http://dx.doi.org/{{publication.doi}}">{{publication.doi}}</a></dd>
                <dt>PMID</dt><dd><a href="http://www.ncbi.nlm.nih.gov/pubmed/{{publication.pmid}}">{{publication.pmid}}</a></dd>
            </dl>
        </li>
    {% endfor %}
            
</ul>
                                                 '''
                                                 }),
                                   ('peopleTypes', {
                                                 'type': 'List',
                                                 'description': 'Which user categories to include in the output - will be done in order',
                                                 'default': ['Contact', 'Public']
                                                 })
                                   ))
        return settingsDef

        
    def run(self):
                    
        studies_datatable_path =  os.path.join(self._config.getSourceDataDir(), "datasets", self._plugin_settings["dataset"], "datatables", self._plugin_settings["datatable"])
    
        if os.path.isdir(studies_datatable_path) != True:
            sys.stdout.write("Making the data directory, i.e. " + studies_datatable_path + '\n')
            os.makedirs(studies_datatable_path)
    
        # Only process studies that are associated with a particular project
        filter_by_project_name = self._plugin_settings["project"]
        
        # Specify the CSV file separators and file path
        csv_value_separator = "\t"
        csv_row_separator = "\n"
        csv_file_path = os.path.join(studies_datatable_path, "data")
        
        # Bail out if the file already exists.
        if os.path.isfile(csv_file_path) == True:
            print("Warning: Overwriting file: " + csv_file_path)
        
        # Open the CSV file for writing
        csv_file = open(csv_file_path, 'w')
        
        # Append the heading line
        csv_file.write(csv_value_separator.join(["Study_number", "webTitle", "description", "publications", "people"]) + csv_row_separator)
        
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
        for node in collaborations["collaborationNodes"]:
            
            # Get the list of projects
            projects = node["projects"]
            
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
            # Study_number    webTitle    description    publications    people
            
            Study_number = node['name'].split('-')[0].replace(csv_value_separator, "")
            
            study_row = []
            study_row.append(Study_number)
            if node["webTitleApproved"] == "false":
                self._log("Warning: webTitle not approved for:" + node['name'])
            study_row.append(node["webTitle"].replace(csv_value_separator, ""))
            
            if node["descriptionApproved"] == "false":
                self._log("Warning: description not approved for:" + node['name'])
            study_row.append(node["description"].replace(csv_value_separator, ""))
            
            template = Template(self._plugin_settings["publicationsTemplate"])
            publications_html = template.render(publications = node["publications"])
            study_row.append(publications_html.replace(csv_value_separator, ""))
            
            template = Template(self._plugin_settings["userTemplate"])
            study_people = self.study_people(people, node)
            people_html = template.render(users = study_people)
            study_row.append(people_html.replace(csv_value_separator, ""))
            
            # Write the study to the CSV file
            csv_file.write(csv_value_separator.join(study_row).encode('ascii', 'xmlcharrefreplace').encode('latin-1').replace("\n", " ").replace("\r", " ") + csv_row_separator)
        
        
        
        # Close the CSV file
        csv_file.close()





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

        # TODO: remove
        print "JSON:"
        print json.dumps(data, indent=2, sort_keys=True)

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
                if malariagenUID in people.keys():
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
                
