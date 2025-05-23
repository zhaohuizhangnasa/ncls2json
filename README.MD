This python script file utilizes the ncls module "walk" function to extract the variable, dimension and the attribute information from netcdf and hdf5 files.

The "walk" function of the ncls module returns there dict varibles: varD, dimD and attD.

There are three functions in this file:

"config_vars" -- reorganize the varD and attD following the config-dict schema

"config_dims" -- reorganize the varDim  following the config-dict schema

"save_config_to_json" -- write config-dict to a json file
