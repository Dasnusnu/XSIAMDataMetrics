import demistomock as demisto
from CommonServerPython import *

def main():
    try:
        # Get inputs from the playbook context
        all_datasets_raw = demisto.args().get('all_datasets_json')
        excluded_datasets_raw = demisto.args().get('excluded_datasets_json')
        exclusion_field = demisto.args().get('dataset_name_field', 'dataset_name')

        # Process the full list of datasets from the API call
        # The API returns a list of dicts, e.g., [{'dataset_name': 'x', 'vendor': 'y'}, ...]
        all_datasets_set = {ds.get('dataset_name') for ds in all_datasets_raw if ds.get('dataset_name')}

        # Process the excluded list from the XQL query
        # The query returns a list of dicts, e.g., [{'dataset_name': 'a'}, {'dataset_name': 'b'}]
        excluded_datasets_set = {ds.get(exclusion_field) for ds in excluded_datasets_raw if ds.get(exclusion_field)}

        # Find the difference using set operations
        final_datasets_list = sorted(list(all_datasets_set - excluded_datasets_set))

        # Return the final list to the playbook context
        return_results(CommandResults(
            outputs_prefix='FinalDatasetList',
            outputs=final_datasets_list,
            readable_output=f"Found {len(final_datasets_list)} datasets after exclusion."
        ))

    except Exception as e:
        return_error(f"Failed to filter datasets. Error: {str(e)}")

if __name__ in ('__main__', '__builtin__', 'builtins'):
    main()