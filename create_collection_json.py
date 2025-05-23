
import netCDF4 as nc4
import os,re,json
import requests
from collections import defaultdict

from ncls import walk

__authors__ = ["Zhaohui Zhang"]
__license__ = "Public Domain"
__maintainer__ = "Zhaohui Zhang"
__email__ = "zhaohui.zhang@nasa.gov"
__date__  = "Dec 12, 2024"


def file_format(input_file):
    """
    Determines the file format based on the file extension.
    
    Args:
        input_file (str): Path to the input file.
    
    Returns:
        dict: File format metadata (label and value).
    """
    file_extension = os.path.splitext(os.path.basename(input_file))[-1]
    fext = file_extension[1:]
    file_info = None 
    fdict = {'hdf5':'HDF5','h5':'HDF5','hdf':'HDF',
            'he5':'HDF-EOS5','he2':'HDF-EOS2',
            'nc': 'netCDF','nc4':'netCDF'
            }
    if fext.lower() in fdict:
        file_info = {"label": fdict[fext.lower()], "value": fext}

    else:
        print("Unsupported file format. Please provide a .he5, .hdf5, .HDF5, or .nc file.")
    return file_info

def collection_umm_json(product, version):
    ''' Returns json-formatted response from an input product short-name and version '''
    #z  cmr_http='https://cmr.earthdata.nasa.gov/search/collections.umm_json'
    cmr_http='https://cmr.uat.earthdata.nasa.gov/search/collections.umm_json'
    #product = 'GPM_2AKu' ; version = '07'
    url = '{}?short-name={}&version={}'.format(cmr_http, product,version)
    result = requests.get(url)
    try:
        result.raise_for_status()
        return json.loads(result.text)
    except :
        return None

def data_related_urls(product, version, Type='GET DATA', Subtype='DATA TREE'):
    response = collection_umm_json(product, version)
    for ru in response['items'][0]['umm']['RelatedUrls']:
       if 'Subtype' not in ru: continue
       if ru['Type'] == Type and ru['Subtype']== Subtype:
          url = re.search(r'(https://)?(.+?)/(.*)', ru['URL'])
          root = url[2] ; subdir = url[3].rstrip('/')
          if subdir:
             subdir = subdir.rstrip('/')

          machine = root.split('.')[0]
          return(root,subdir)


def get_var_values(var, fid):
    if var.startswith("/"): var = var[1:]
    groups = var.split('/')
    var_name = groups[-1]
    if not var_name: return

    ds = fid
    if len(groups) > 1:
        # Access the desired group
        for g in groups[0:-1]: ds = ds.groups[g]

    # Access the variable within the group
    variable = ds.variables[var_name]

    return variable

def config_global_attrs(fid, product=None, version=None, **kwargs):

    global_attrs ={key: fid.getncattr(key) for key in fid.ncattrs()}
    server_url, subdir = data_related_urls(product, version, Type='GET DATA', Subtype='DATA TREE')
    prod_url = "{}/{}/".format(server_url, subdir)
    
    url_re = server_url
    prod_url_re = prod_url
    for s in "./,":
       url_re = url_re.replace(s,'\\'+s)
       prod_url_re = prod_url_re.replace(s,'\\'+s)

    global_meta = {}
    global_meta["product"] = product
    global_meta["server"] =  server_url
    global_meta["agentId"] = "SUBSET_LEVEL2"
    global_meta["fileIdMappings"] = [
        {
            "matchRegExpr": prod_url_re,
            "substituteExpressions": [
                {
                    "substitute": prod_url_re,
                    "with": prod_url
                }
            ],
            "type": "url"
        }
    ]
    #"SOUNDER" "SWATH"
    gridType = 'SWATH'
    global_meta["gridType"] =  gridType
    global_meta["presentation"] = [
        {
            "label": "Spatial dimensions will be trimmed to data. (Default)",
            "value": "CROP"
        },
        {
            "label": "Spatial dimensions will remain at original lengths.",
            "value": "FULL"
        },
        {
            "label": "Spatial dimensions will be reduced to a single data stream dimension.",
            "value": "VECTOR"
        }
    ]

    global_meta["spatial"] = ["bbox","circle","point"]
    global_meta["time"] = []
    global_meta["timeName"] = []
    global_meta["timeAggregation"] = []
    global_meta["forceTimeParameter"] = True

    global_meta["version"] = version

    return global_meta



def config_vars(varD, attD, dimD):

    # this function is used to reconstruct the variable dict (varD)
    # into a list of sub-dict following the json schema
    variables = []

    for var in list(varD.keys()):
        # Dimensions and their sizes
        var_value = var
        var_name = os.path.basename(var_value)
        group_path = varD[var]['path']

        attrs = attD[var]
        vardimL = list(varD[var]['dimensions'])
        dimensions_for_var_all = [os.path.join(group_path, dim) for dim in vardimL]
        dimensions_for_var = [ dim for dim in dimensions_for_var_all if 'pres' in dim ]
        
        if 'long_name' in list(attrs):
            long_name = attrs['long_name']
            var_label = '{} ({})'.format(var_name, long_name)
        else:
            var_label = var_name
        if var in dimD.keys(): continue

        if len(dimensions_for_var_all) > 0:
           variables.append({
            "additionalParameters": [],
            'value': var_value,
            'label': var_label,
            'dimensions': dimensions_for_var,
            })
        else:
           variables.append({
            'value': var_value,
            'label': var_label 
            })

    return variables


def config_dims(dimD, varD, attD, fid): 
    # this function is used to reconstruct the dimension dict variable (dimD)
    # into a list of sub-dict following the json schema
    dimensions = []

    for key in list(dimD.keys()):
        dim_size = dimD[key]['size']
        dim_values = [{'value': str(i), 'label': str(i+1)} for i in range(dim_size)]
        unit = ''
        dim_label = os.path.basename(key)
        if key in varD:
           if 'long_name' in list(attD[key]):
            dim_label = attD[key]['long_name']

           dim_var =  get_var_values(key, fid)
           if 'units' in attD[key]:
               unit = attD[key]['units']
               if unit != 'none' and unit != '1':
                 for i in range(dim_size):
                      dim_values[i]['label'] = dim_values[i]['label'] + f' ({dim_var[i]:.01f} {unit})'
        if 'pres' in dim_label:
           dimensions.append({
                'value': key,
                'label': dim_label,
                'dimensionValues': dim_values,  
               })

    return dimensions

def save_config_to_json(config, output_json):
    """
    Save config metadata to a JSON file.
    
    Ajson.dumprgs:
        config (dict): The metadata to be saved.
        output_json (str): The name of the output JSON file.
    """
    try:
        with open(output_json, 'w') as json_file:
            json.dump(config, json_file, indent=4)
            print(f"Config saved to {output_json}")
    except Exception as e:
        print(f"Error in saving config to JSON: {e}")

def main():
    """
    Main function to parse command-line arguments and process the input file.
    
    It reads a NetCDF/HDF file, extracts metadata and dimensions, 
    and saves the results to a JSON file.
    """
    import argparse
    parser = argparse.ArgumentParser(description="Extract metadata from NetCDF files.")
    parser.add_argument('input_file', type=str, help="Path to the input file (.nc)")
    parser.add_argument('-p', '--product', type=str, default='GPM_2AKa', help="product name")
    parser.add_argument('--version', type=str, default='07', help="product version")
    parser.add_argument('-o', '--output', type=str, default='metadata_config.json', help="Output JSON file name")
    parser.add_argument('-v', '--verbose', action='store_true', help="Enable verbose mode (more logging)")

    args = parser.parse_args()

    input_file = args.input_file
    output_json = args.output
    output_xml = 'output.xml'
    product = args.product
    version = args.version
    verbose = args.verbose


    if verbose:
        print(f"Verbose mode enabled.")
        print(f"Input file: {input_file}")
        print(f"Output file: {output_json}")
    
    # Check if the file type is valid
    file_type = file_format(input_file)
    if not file_type:
       print("{}: not a valid netcdf or hdf file ...".format(input_file))
       return


    # Initialize stuff
    hasGroups = -1
    grpL = []
    dimD = defaultdict(dict)
    varD = defaultdict(dict)
    attD = defaultdict(dict)

    # Open the file and walk through it
    """Extract global metadata (attributes) from a file."""
    try:
        fid = nc4.Dataset(input_file,'r')
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return None
    hasGroups = walk(fid,'',dimD, varD, attD, hasGroups, grpL)

    # collect global/common config 
    global_attrs = config_global_attrs(fid, product=product, version=version)

    # collect config variables
    variables = config_vars(varD, attD, dimD)

    # collect config dimensions
    dimensions = config_dims(dimD, varD, attD, fid)

    fid.close()

    # initial config variable 
    config = global_attrs if global_attrs else {}

    # Add file type to config
    config["format"] =  [file_type]

    # Add dimensions and variables to config
    if dimensions:
       config['dimension'] = dimensions

    if variables:
       config['variable'] = variables

    # Save config to JSON
    if config:
       save_config_to_json(config, output_json)

if __name__ == '__main__':
    main()
