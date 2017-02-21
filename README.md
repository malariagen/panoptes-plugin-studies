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

Create a directory in `datasets/yourDataset/pre/` e.g. `datasets/myDataset/pre/studies/`

In there, create a file named `settings`, containing the settings values for this plugin. See the example `settings` file in `dataset_example/Studies/datatables/pre/studies/` and the `getSettings` method in `studyDetails.py` for clues.

The `dataset_example` directory illustrates an example of the expected dataset structure and includes example `settings` files.

When the Panoptes' dataset import is run, files named `data` will be created in the three datatable directories, according to this plugin's settings. For example, a file named `data` containing the study records will be created in `datatables/studies/`. Each `data` file will be imported into Panoptes in the usual way.
