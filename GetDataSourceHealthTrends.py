# Script Name: GetDataSourceHealthTrends
# Description: Analyzes trends from the custom dataset
# Tags: ['health', 'monitoring', 'trends']
# Type: python3

import datetime
from typing import Dict, List

def get_health_trends(dataset_name: str = None, timeframe_hours: int = 24) -> Dict:
    """
    Get health trends from the custom dataset
    """
    
    # Build query based on parameters
    dataset_filter = f'| filter dataset_name = "{dataset_name}"' if dataset_name else ''
    
    xql_query = f"""
    config timeframe = {timeframe_hours}h
    | dataset = custom.data_source_health
    {dataset_filter}
    | bin check_time span=1h as hour_bucket
    | stats 
        avg(hours_silent) as avg_hours_silent,
        max(hours_silent) as max_hours_silent,
        count() as check_count,
        countif(status = "Silent") as silent_count
      by hour_bucket, dataset_name
    | alter silent_percentage = (silent_count / check_count) * 100
    | sort hour_bucket asc
    """
    
    try:
        results = demisto.executeCommand('xdr-xql-generic-query', {
            'query': xql_query
        })
        
        if not results or results[0].get('Type') == entryTypes['error']:
            return_error(f"Failed to get trends: {results[0].get('Contents')}")
        
        trend_data = results[0].get('Contents', {}).get('results', [])
        
        # Analyze trends
        trending_worse = []
        trending_better = []
        
        # Group by dataset
        dataset_trends = {}
        for row in trend_data:
            ds_name = row.get('dataset_name')
            if ds_name not in dataset_trends:
                dataset_trends[ds_name] = []
            dataset_trends[ds_name].append(row)
        
        # Analyze each dataset
        for ds_name, trends in dataset_trends.items():
            if len(trends) >= 2:
                # Compare recent vs older
                recent_avg = sum(t['avg_hours_silent'] for t in trends[-3:]) / min(3, len(trends))
                older_avg = sum(t['avg_hours_silent'] for t in trends[:-3]) / max(1, len(trends) - 3)
                
                if recent_avg > older_avg * 1.5:
                    trending_worse.append({
                        'dataset': ds_name,
                        'recent_avg_silent': round(recent_avg, 2),
                        'older_avg_silent': round(older_avg, 2),
                        'change_percent': round(((recent_avg - older_avg) / older_avg) * 100, 1)
                    })
                elif recent_avg < older_avg * 0.5:
                    trending_better.append({
                        'dataset': ds_name,
                        'recent_avg_silent': round(recent_avg, 2),
                        'older_avg_silent': round(older_avg, 2),
                        'change_percent': round(((recent_avg - older_avg) / older_avg) * 100, 1)
                    })
        
        return {
            'raw_trends': trend_data,
            'trending_worse': trending_worse,
            'trending_better': trending_better,
            'dataset_count': len(dataset_trends)
        }
        
    except Exception as e:
        return_error(f"Error analyzing trends: {str(e)}")

# Main execution
def main():
    args = demisto.args()
    dataset_name = args.get('dataset_name')
    timeframe_hours = int(args.get('timeframe_hours', 24))
    
    trends = get_health_trends(dataset_name, timeframe_hours)
    
    # Create readable output
    readable = f"## Data Source Health Trends ({timeframe_hours}h)\n\n"
    
    if trends['trending_worse']:
        readable += "### Trending Worse\n"
        for ds in trends['trending_worse']:
            readable += f"- **{ds['dataset']}**: {ds['change_percent']}% increase in silence periods\n"
        readable += "\n"
    
    if trends['trending_better']:
        readable += "### Trending Better\n"
        for ds in trends['trending_better']:
            readable += f"- **{ds['dataset']}**: {ds['change_percent']}% decrease in silence periods\n"
        readable += "\n"
    
    readable += f"*Analyzed {trends['dataset_count']} data sources*"
    
    return_outputs(
        readable_output=readable,
        outputs={'HealthTrends': trends}
    )

if __name__ in ['__main__', '__builtin__', 'builtins']:
    main()
