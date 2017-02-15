Before installing the `REQUIREMENTS` you will need to install the LDAP libraries, e.g.:

```bash
sudo apt-get install libldap2-dev libsasl2-dev
```

Then install the python packages listed in the `REQUIREMENTS` file into Panoptes' virtual environment, e.g.:

```bash
source /path/to/panoptes_virtualenv/bin/activate
pip install -r ~/path/to/REQUIREMENTS
deactivate
```

Usage
=====

Install the plugin either by setting `PLUGINPATH` in the panoptes config or by copying the `studyDetails` directory to the `server/responders/importer/plugins` directory

Create a directory under <<dataset>>/pre e.g. <<dataset>>/pre/studies

In there create a settings file with the plugin settings values. See the `settings.example` file and the `getSettings` method in `studyDetails.py` for clues.

When panoptes is run a data file will be created according to the settings which can be loaded in the usual way.


Error Messages
==============

```
Error: No JSON object could be decoded
```
A failed attempt to authenticate (a bad `userId` or `password`) when requesting the `studiesURL` will return a HTML response, rather than the expected JSON.
