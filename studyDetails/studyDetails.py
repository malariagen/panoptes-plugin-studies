from __future__ import unicode_literals

import csv
import json
import os
import sys
import urllib2
from collections import OrderedDict
from os.path import join, isdir, isfile

import ldap
import ldap.sasl
from responders.importer.BasePlugin import BasePlugin

# FIXME: If a setting is required ('required': True) and no value is provided
# in the settings file, an error occurs despite a default being provided here.
# An error should only occur if no default has been provided for those required.
# Workaround: do not set required properties for settings that have defaults.

class studyDetails(BasePlugin):

    def getSettings(self):
        settingsDef = OrderedDict((('plugin', {
                                               'type': 'Text',
                                               'description': "The name of the plugin class to run.",
                                               'values':  { 'studyDetails': { 'description' : 'The name of the plugin class'} },
                                               'required': True
                                               }),

                                   ('ldapServer', {
                                                 'type': 'Text',
                                                 'description': 'The URI of the LDAP server, using the ldap:// or ldaps:// scheme.',
                                                 'required': True
                                                 }),
                                   ('ldapUserDN', {
                                                 'type': 'Text',
                                                 'description': 'The LDAP DN of the user for the LDAP connection. This user will require the appropriate access permissions to read people records.',
                                                 'required': True
                                                 }),
                                   ('ldapUserPassword', {
                                                 'type': 'Text',
                                                 'description': 'The password for the LDAP user specified by ldapUserDN.',
                                                 'required': True
                                                 }),

                                   ('alfrescoUserId', {
                                                 'type': 'Text',
                                                 'description': 'The Alfresco user id for the Alfresco login, used to retrieve the study information from Alfresco.',
                                                 'required': True
                                                 }),
                                   ('alfrescoUserPassword', {
                                                 'type': 'Text',
                                                 'description': 'The password for the Alfresco user specified by alfrescoUserId.',
                                                 'required': True
                                                 }),
                                   ('alfrescoStudiesURL', {
                                                 'type': 'Text',
                                                 'description': 'The URL in Alfresco to fetch the study descriptions from.',
                                                 'default': 'https://alfresco.malariagen.net/alfresco/service/cggh/collaborations',
                                                 }),
                                   ('alfrescoProject', {
                                                 'type': 'Text',
                                                 'description': 'The name of the Alfresco project to restrict studies by. When this defaults to None, all studies accessible to alfrescoUserId will be processed.',
                                                 'default': None
                                                 }),
                                   ('alfrescoPeopleGroups', {
                                                 'type': 'List',
                                                 'description': 'The list of user groups from Alfresco to include in the studyPeopleDatatable, in the same order.',
                                                 'default': ['Contact', 'Public'],
                                                 'required': False
                                                 }),

                                   ('alfrescoWebStudyHandling', {
                                                 'type': 'Text',
                                                 'description': 'The option for handling studies that have a webStudy link in Alfresco, which are subsets of another specified study.',
                                                 'default': 'ignore',
                                                 'values':  ['ignore', 'keep', 'merge']
                                                 }),


                                   ('dataset', {
                                                 'type': 'Text',
                                                 'description': 'The name of the Panoptes dataset',
                                                 'required': True
                                                 }),
                                   ('studiesDatatable', {
                                                 'type': 'Text',
                                                 'description': 'The name of the Panoptes datatable where the Alfresco study records will be written.',
                                                 'default': 'studies',
                                                 }),
                                   ('studyPeopleDatatable', {
                                                 'type': 'Text',
                                                 'description': 'The name of the Panoptes datatable where the people records associated with each Alfresco study will be written.',
                                                 'default': 'study_people',
                                                 }),
                                   ('studyPublicationsDatatable', {
                                                 'type': 'Text',
                                                 'description': 'The name of the Panoptes datatable where the publication records associated with each Alfresco study will be written.',
                                                 'default': 'study_publications',
                                                 }),
                                   ('studySamplesDatatable', {
                                                 'type': 'Text',
                                                 'description': "The name of the Panoptes datatable from which the sample records associated with each Alfresco study will be read. Only the Alfresco studies that correspond to these records will be used. These records are also used to rewrite the study ids.",
                                                 'default': None,
                                                 'required': False
                                                 }),
                                   ('sampleCountriesDatatable', {
                                                 'type': 'Text',
                                                 'description': 'The name of the Panoptes datatable where the sampleCountriesFields from studySamplesDatatable will be written.',
                                                 'default': 'sample_countries',
                                                 }),

                                   ('studyPeopleFields', {
                                                 'type': 'List',
                                                 'description': 'The list of people fields from Alfresco to include in the studyPeopleDatatable, in the same order. The field names will need to match both the data from LDAP and the settings file for the studyPeopleDatatable.',
                                                 'default': ['jobTitle1', 'jobTitle2', 'jobTitle3', 'uid', 'researchGateURL', 'scholarURL', 'twitterURL', 'malariagenUID', 'oProfile1', 'oProfile2', 'oProfile3', 'ORCID', 'sn', 'mail', 'givenName', 'o1', 'o2', 'o3'],
                                                 }),
                                   ('studyPublicationsFields', {
                                                 'type': 'List',
                                                 'description': 'The list of  publication fields from Alfresco to include in the studyPublicationsDatatable, in the same order. The field names will need to match both the data from Alfresco and the settings file for the studyPublicationsDatatable.',
                                                 'default': ['doi', 'name', 'title', 'citation', 'pmid'],
                                                 }),
                                   ('sampleCountriesFields', {
                                                 'type': 'List',
                                                 'description': 'The list of country fields from studySamplesDatatable to copy to sampleCountriesDatatable. Since there are multiple sample records for each country, the values of each field from the last sample record are used. Therefore, ensure that the values for these country fields are the same for all records with the same samplesCountryColumn value.',
                                                 'default': ['CountryCode', 'CountryName'],
                                                 }),

                                   ('studyPeoplePersonField', {
                                                 'type': 'Text',
                                                 'description': 'The uniquely identifying field for a person in the studyPeopleDatatable.',
                                                 'default': 'uid',
                                                 }),
                                   ('studyPublicationsPublicationField', {
                                                 'type': 'Text',
                                                 'description': 'The uniquely identifying field for a publication in the studyPublicationsDatatable.',
                                                 'default': 'pmid',
                                                 }),
                                   ('studySamplesStudyField', {
                                                'type': 'Text',
                                                'description': 'The uniquely identifying field for a study in the studySamplesDatatable.',
                                                'default': 'StudyCode',
                                                'required': False
                                                }),
                                   ('studySamplesCountryField', {
                                                'type': 'Text',
                                                'description': 'The uniquely identifying field for a country in the studySamplesDatatable.',
                                                'default': 'CountryCode',
                                                'required': False
                                                })
        ))

        return settingsDef


    def run(self):

        # Determine the paths to the datatable directories.
        datatables_path = join(self._config.getSourceDataDir(), "datasets", self._plugin_settings["dataset"], "datatables")
        studies_datatable_path = join(datatables_path, self._plugin_settings["studiesDatatable"])
        study_people_datatable_path = join(datatables_path, self._plugin_settings["studyPeopleDatatable"])
        study_publications_datatable_path = join(datatables_path, self._plugin_settings["studyPublicationsDatatable"])
        sample_countries_datatable_path = join(datatables_path, self._plugin_settings["sampleCountriesDatatable"])

        # Only process studies that are associated with a particular project or samples list
        filter_by_project_name = self._plugin_settings["alfrescoProject"]
        samples_table = self._plugin_settings['studySamplesDatatable']
        samples_study_column = self._plugin_settings['studySamplesStudyField']
        samples_country_column = self._plugin_settings['studySamplesCountryField']
        sample_countries_fields = self._plugin_settings['sampleCountriesFields']

        # Create the datatable directories, if they don't already exist.
        if not isdir(studies_datatable_path):
            sys.stdout.write("Making the studies datatable directory, i.e. " + studies_datatable_path + '\n')
            os.makedirs(studies_datatable_path)
        if not isdir(study_people_datatable_path):
            sys.stdout.write("Making the study_people datatable directory, i.e. " + study_people_datatable_path + '\n')
            os.makedirs(study_people_datatable_path)
        if not isdir(study_publications_datatable_path):
            sys.stdout.write("Making the study_publications datatable directory, i.e. " + study_publications_datatable_path + '\n')
            os.makedirs(study_publications_datatable_path)
        if samples_table is not None and not isdir(sample_countries_datatable_path):
            sys.stdout.write("Making the sample_countries datatable directory, i.e. " + sample_countries_datatable_path + '\n')
            os.makedirs(sample_countries_datatable_path)


        # Specify the CSV file item separators.
        csv_value_separator = "\t"
        csv_row_separator = "\n"
        csv_list_separator = "; "

        # Determine the paths to the data files.
        studies_csv_file_path = join(studies_datatable_path, "data")
        study_people_csv_file_path = join(study_people_datatable_path, "data")
        study_publications_csv_file_path = join(study_publications_datatable_path, "data")
        sample_countries_csv_file_path = join(sample_countries_datatable_path, "data")

        # Print a warning if any of the data files already exist.
        if isfile(studies_csv_file_path):
            print("Warning: Overwriting file: " + studies_csv_file_path)
        if isfile(study_people_csv_file_path):
            print("Warning: Overwriting file: " + study_people_csv_file_path)
        if isfile(study_publications_csv_file_path):
            print("Warning: Overwriting file: " + study_publications_csv_file_path)
        if isfile(sample_countries_csv_file_path):
            print("Warning: Overwriting file: " + sample_countries_csv_file_path)

        # Open the CSV files for writing.
        studies_csv_file = open(studies_csv_file_path, 'w')
        study_publications_csv_file = open(study_publications_csv_file_path, 'w')
        study_people_csv_file = open(study_people_csv_file_path, 'w')

        # Append the heading lines.
        studies_csv_file.write(csv_value_separator.join(["study", "study_number", "webTitle", "description"]) + csv_row_separator)
        study_publications_csv_file.write(csv_value_separator.join(["study"] + self._plugin_settings["studyPublicationsFields"]) + csv_row_separator)
        study_people_csv_file.write(csv_value_separator.join(["study"] + self._plugin_settings["studyPeopleFields"]) + csv_row_separator)

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

        # Collect the studies by name, to facilitate a subsequent parse.
        studiesByName = {}
        for study in collaborations["collaborationNodes"]:
          if study["name"] not in studiesByName:
            studiesByName[study["name"]] = study
          else:
            self._log("Warning: duplicate study name:" + study['name'])

        #Rewrite the samples table based on the "webStudy" field and to note countries data
        countryByISO = {}
        wanted_studies = None
        if samples_table is not None:
            reported = {}
            wanted_studies = set()
            samples_table_path = join(datatables_path, samples_table, 'data')
            if isfile(samples_table_path):
                print("Warning: Overwriting file: " + samples_table_path)
            with open(samples_table_path) as tsv:
                reader = csv.DictReader(tsv, delimiter=str(csv_value_separator))
                rows = list(reader)
                for row in rows:
                    name = row[samples_study_column]
                    if name in studiesByName and 'webStudy' in studiesByName[name]:
                        if name not in reported:
                            print(name +' will be shown as '+ studiesByName[name]['webStudy']['name'])
                            reported[name] = True
                        name = studiesByName[name]['webStudy']['name']
                    row[samples_study_column + '_short'] = self.getStudyNumber(name, csv_value_separator)
                    row[samples_study_column] = name
                    wanted_studies.add(name)
                    if samples_country_column:
                        countryByISO[row[samples_country_column]] = {column: row[column] for column in sample_countries_fields}
                        countryByISO[row[samples_country_column]][samples_country_column] = row[samples_country_column]

            fieldnames = list(set(reader.fieldnames + [samples_study_column + '_short']))
            with open(samples_table_path, 'w') as tsv:
                writer = csv.DictWriter(tsv, fieldnames=fieldnames, delimiter=str(csv_value_separator))
                writer.writeheader()
                writer.writerows(rows)
            if samples_country_column is not None:
                with open(sample_countries_csv_file_path, 'w') as tsv:
                    writer = csv.DictWriter(tsv, fieldnames=sample_countries_fields, delimiter=str(csv_value_separator))
                    writer.writeheader()
                    writer.writerows(countryByISO.values())


        ## Loop through the collaboration nodes (each represents a study)

        # Collect data for a subsequent parse.
        substudiesByParentName = {}
        personIdsByParentName = {}
        publicationIdsByParentName = {}

        for study in collaborations["collaborationNodes"]:

            # Get the list of projects
            projects = study["projects"]

            # See if this study is in the project we want
            # by looking through the list of associated project names.
            is_in_project = True if filter_by_project_name is None else False
            for project in projects:

                project_name = project["name"]

                if project_name == filter_by_project_name:
                    is_in_project = True
                    break

            # If this study isn't in the project, then skip to the next study.
            if not is_in_project:
                continue

            # Skip this study if it is not in the list of wanted_studies.
            if wanted_studies is not None and study['name'] not in wanted_studies:
                continue

            if wanted_studies is not None:
              wanted_studies.remove(study['name'])

            if self._plugin_settings["alfrescoWebStudyHandling"] != "keep":
              # Anything with a webStudy is a subset of another study.
              # Collect the substudy and process it in a subsequent parse.
              if "webStudy" in study:
                if study["webStudy"]["name"] not in substudiesByParentName:
                  substudiesByParentName[study["webStudy"]["name"]] = []
                substudiesByParentName[study["webStudy"]["name"]].append(study)
                continue

            # Compose the study row, which will be appended to the CSV file
            # study	study_number    webTitle    description

            study_id = study['name']
            study_number = self.getStudyNumber(study['name'], csv_value_separator)

            study_row = [study_id, study_number]
            if study["webTitleApproved"] == "false":
                self._log("Warning: webTitle not approved for:" + study['name'])
            study_row.append(study["webTitle"].replace(csv_value_separator, ""))

            if study["descriptionApproved"] == "false":
                self._log("Warning: description not approved for:" + study['name'])
            study_row.append(study["description"].replace(csv_value_separator, ""))

            study_people = self.study_people(people, study)

            # Write the related records: people and publications.
            personIdsByParentName[study['name']] = self.writeRelatedRecords(study_people, study_id, self._plugin_settings["studyPeopleFields"], study_people_csv_file, csv_list_separator, csv_row_separator, csv_value_separator, self._plugin_settings["studyPeopleKeyField"])
            publicationIdsByParentName[study['name']] = self.writeRelatedRecords(study["publications"], study_id, self._plugin_settings["studyPublicationsFields"], study_publications_csv_file, csv_list_separator, csv_row_separator, csv_value_separator, self._plugin_settings["studyPublicationsKeyField"])

            # Write the study to the CSV file
            studies_csv_file.write(csv_value_separator.join(study_row).encode('ascii', 'xmlcharrefreplace').encode('latin-1').replace("\n", " ").replace("\r", " ") + csv_row_separator)
        if wanted_studies is not None and len(wanted_studies) > 0:
            raise Exception('These studies were not found', str(wanted_studies))

        if self._plugin_settings["alfrescoWebStudyHandling"] == "merge":
          for substudyParentName in substudiesByParentName:
            print "substudyParentName: " + substudyParentName
            parentStudy = studiesByName[substudyParentName]
            study_id = parentStudy['name']
            for substudy in substudiesByParentName[substudyParentName]:
              study_people = self.study_people(people, substudy)
              new_study_people = []
              new_study_publications = []
              for study_person in study_people:
                if study_person[self._plugin_settings["studyPeopleKeyField"]] not in personIdsByParentName[substudyParentName]:
                  new_study_people.append(study_person)
              for study_publication in substudy["publications"]:
                if study_publication[self._plugin_settings["studyPublicationKeyField"]] not in publicationIdsByParentName[substudyParentName]:
                  new_study_publications.append(study_publication)
              if new_study_people:
                added_people = self.writeRelatedRecords(new_study_people, study_id, self._plugin_settings["studyPeopleFields"], study_people_csv_file, csv_list_separator, csv_row_separator, csv_value_separator, self._plugin_settings["studyPeopleKeyField"])
              if new_study_publications:
                added_publications = self.writeRelatedRecords(new_study_publications, study_id, self._plugin_settings["studyPublicationsFields"], study_publications_csv_file, csv_list_separator, csv_row_separator, csv_value_separator, self._plugin_settings["studyPublicationsKeyField"])

        # Close the CSV file
        studies_csv_file.close()


    def getStudyNumber(self, study_name, csv_value_separator):
      return study_name.split('-')[0].replace(csv_value_separator, "")

    def writeRelatedRecords(self, records, foreign_key_value, fields, file, csv_list_separator, csv_row_separator, csv_value_separator, collection_field):
      collected_values = []
      for record in records:
          record_values = []
          record_values.append(foreign_key_value)
          for record_field in fields:
              record_field_value = ''
              if record_field in record:
                  if isinstance(record[record_field], basestring):
                      record_field_value = record[record_field]
                      if record_field == collection_field:
                        collected_values.append(record_field_value)
                  elif isinstance(record[record_field], (list, tuple)):
                      record_field_value = csv_list_separator.join(record[record_field])
              record_values.append(record_field_value)
          # Write the record_values to the CSV file
          file.write(csv_value_separator.join(record_values).encode('ascii', 'xmlcharrefreplace').encode('latin-1').replace("\n", " ").replace("\r", " ") + csv_row_separator)
      return collected_values

    def fetchDetails(self):

        self._calculationObject.SetInfo('Fetching study details')
        # create a password manager
        password_mgr = urllib2.HTTPPasswordMgrWithDefaultRealm()

        top_level_url = self._plugin_settings["alfrescoStudiesURL"]
        password_mgr.add_password(None, top_level_url, self._plugin_settings["alfrescoUserId"], self._plugin_settings["alfrescoUserPassword"])

        handler = urllib2.HTTPBasicAuthHandler(password_mgr)

        # create "opener" (OpenerDirector instance)
        opener = urllib2.build_opener(handler)

        json_url = self._plugin_settings["alfrescoStudiesURL"]
        # use the opener to fetch a URL

        json_file = opener.open(json_url)

        data = json.load(json_file)

        #print json.dumps(data, indent=2, sort_keys=True)

        json_file.close()

        return data

    def open_ldap(self):
        server = self._plugin_settings["ldapServer"]
        user_dn = self._plugin_settings['ldapUserDN']
        user_pw = self._plugin_settings['ldapUserPassword']

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

        for group_type in self._plugin_settings["alfrescoPeopleGroups"]:
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
