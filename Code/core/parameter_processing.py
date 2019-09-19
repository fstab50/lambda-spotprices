"""
Summary.

    ParameterSet Class

"""
import os
import sys
import json
import pdb


def read(fname):
    """
    Summary.

        Read and process file object

    """
    basedir = os.path.dirname(sys.argv[0])
    return open(os.path.join(basedir, fname)).read()


class ParameterSet():
    """Recursion class for processing complex dictionary schema."""

    def __init__(self, parameters):
        """
        Summary.

            Retains major and minor version numbers + parameters
            in json form for later use

        Args:
            :parameters (str): path to json file obj containing
             parameter keys and values.  Alternatively, parameters may
             be a json dictionary
            :version (str): current build version
        """
        self.container = []
        self.inner = {}
        self.publicationDate = None

        if isinstance(parameters, dict):
            self.parameter_dict = parameters
        elif isinstance(parameters, str):
            self.parameter_dict = json.loads(parameters)
        elif os.path.isfile(parameters):
            self.parameter_dict = json.loads(read(parameters))
        else:
            self.parameter_dict = {}

    def create(self, parameters=None):
        """
        Summary.

            Update parameter dict with current values appropriate
            for the active build

        Args:
            :parameters (dict): dictionary of all parameters used to gen rpm
            :version (str):  the version of the current build, e.g. 1.6.7

        Returns:
            parameters, TYPE: dict

        """
        if parameters is None:
            parameters = self.parameter_dict

        if parameters.get('publicationDate'):
            self.publicationDate = parameters['publicationDate']

        for k, v in parameters.items():

            if k == 'sku':
                sku = v
                self.inner[k] = v
                self.inner['publicationDate'] = self.publicationDate

            elif k == 'productFamily':
                self.inner[k] = v

            elif k == 'attributes' and v.get('instanceType'):
                instance_type = v['instanceType']
                self.inner['size'] = instance_type
                self.inner[k] = v
                self.container.append(self.inner)
                self.inner = {}

            elif isinstance(v, dict):
                self.create(v)

        return self.container
