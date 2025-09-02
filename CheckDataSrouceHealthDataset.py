# Script Name: CheckDataSourceHealthDataset
# Description: Reads from the custom dataset instead of running expensive queries
# Tags: ['health', 'monitoring', 'data-source', 'optimized']
# Type: python3

import datetime
from typing import Dict, List

def check_health_dataset(threshold_hours: float = 1.0) -> List[Dict]:
    """
    Check the custom dataset for silent data sources
    This is much more CU-efficient than querying raw data
    """
    
    # Query the custom dataset (very low CU consumption)
    xql_query = f"""
    config timeframe = 1h
    | dataset = custom.data_source_health
    | filter hours_silent >= {threshold_hours}
    | sort hours_silent desc
    | fields dataset_name, last_seen_time, hours_silent, severity, 
             event_count_last_hour, status, check_time
    """
    
    try:
        results = demisto.executeCommand('xdr-xql-generic-query', {
            'query': xql_query,
            'time_frame': 'custom',
            'start_time': '1 hour ago'
        })
        
        if not results or results[0].get('Type') == entryTypes['error']:
            return_error(f"Failed to query health dataset: {results[0].get('Contents')}")
        
        query_results = results[0].get('Contents', {}).get('results', [])
        
        # Process and format results
        silent_datasets = []
        for row in query_results:
            dataset_info = {
                'dataset': row.get('dataset_name'),
                'last_seen': datetime.datetime.fromtimestamp(
                    row.get('last_seen_time', 0)
                ).strftime('%Y-%m-%d %H:%M:%S UTC'),
                'last_seen_timestamp': row.get('last_seen_time', 0),
                'hours_silent': row.get('hours_silent', 0),
                'severity': row.get('severity', 'Unknown'),
                'event_count': row.get('event_count_last_hour', 0),
                'status': row.get('status', 'Unknown'),
                'last_check': datetime.datetime.fromtimestamp(
                    row.get('check_time', 0)
                ).strftime('%Y-%m-%d %H:%M:%S UTC')
            }
            silent_datasets.append(dataset_info)
        
        return silent_datasets
        
    except Exception as e:
        return_error(f"Error checking health dataset: {str(e)}")

def verify_dataset_freshness() -> Dict:
    """
    Verify the custom dataset has recent data
    """
    freshness_query = """
    config timeframe = 2h
    | dataset = custom.data_source_health
    | comp max(check_time) as latest_check
    | alter minutes_old = (current_time() - latest_check) / 60
    | fields latest_check, minutes_old
    """
    
    try:
        results = demisto.executeCommand('xdr-xql-generic-query', {
            'query': freshness_query
        })
        
        if results and results[0].get('Type') != entryTypes['error']:
            data = results[0].get('Contents', {}).get('results', [])
            if data:
                return {
                    'is_fresh': data[0].get('minutes_old', 999) < 35,
                    'minutes_old': data[0].get('minutes_old', 999),
                    'last_update': data[0].get('latest_check')
                }
        
        return {'is_fresh': False, 'minutes_old': 999, 'last_update': None}
        
    except:
        return {'is_fresh': False, 'minutes_old': 999, 'last_update': None}

# Main execution
def main():
    args = demisto.args()
    threshold_hours = float(args.get('threshold_hours', 1))
    
    # First verify dataset freshness
    freshness = verify_dataset_freshness()
    
    if not freshness['is_fresh']:
        return_error(f"Health dataset is stale. Last update was {freshness['minutes_old']} minutes ago")
    
    # Get silent data sources
    silent_datasets = check_health_dataset(threshold_hours)
    
    # Create output
    if silent_datasets:
        readable = f"## Found {len(silent_datasets)} Silent Data Sources\n\n"
        readable += f"*Dataset last updated: {freshness['minutes_old']:.1f} minutes ago*\n\n"
        readable += "| Dataset | Last Seen | Hours Silent | Severity | Status |\n"
        readable += "|---------|-----------|--------------|----------|--------|\n"
        
        for ds in silent_datasets:
            readable += f"| {ds['dataset']} | {ds['last_seen']} | {ds['hours_silent']} | {ds['severity']} | {ds['status']} |\n"
    else:
        readable = "All data sources are reporting normally.\n"
        readable += f"*Checked at: {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}*"
    
    return_outputs(
        readable_output=readable,
        outputs={
            'SilentDataSources': silent_datasets,
            'DatasetFreshness': freshness
        },
        raw_response=silent_datasets
    )

if __name__ in ['__main__', '__builtin__', 'builtins']:
    main()
