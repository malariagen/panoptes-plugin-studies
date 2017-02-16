Before installing the `REQUIREMENTS` you will need to install the LDAP libraries, e.g.:

```bash
sudo apt-get install libldap2-dev libsasl2-dev
```

Then install the python packages listed in the `REQUIREMENTS` file into Panoptes' virtual environment, e.g.:

```bash
source /path/to/panoptes_virtualenv/bin/activate
pip install -r /path/to/REQUIREMENTS
deactivate
```

Usage
=====

Install this plugin either by setting `PLUGINPATH` in the Panoptes config or by copying the `studyDetails` directory to the `server/responders/importer/plugins` directory

Create a directory under <<dataset>>/pre e.g. <<dataset>>/pre/studies

In there, create a file named `settings`, containing the settings values for this plugin. See the `plugin.settings.example` file and the `getSettings` method in `studyDetails.py` for clues.

When Panoptes is run, a file named `data` will be created in the `datatable` directory, according to this plugin's settings. The `data` file will be imported into Panoptes in the usual way.

See the `datatable.settings.example` file for a set of example settings that match the `data` file created by this plugin.
