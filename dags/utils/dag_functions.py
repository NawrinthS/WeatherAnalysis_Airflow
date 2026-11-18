from datetime import datetime, timedelta

def get_conf_date(**context):
    """
    Retrieves execution date from dag_run.conf if available,
    otherwise defaults to current UTC date minus 1 day,
    and pushes date formats to XCom via return value.
    """
    dag_run = context.get('dag_run')
    
    # Check if a config was passed with run_date
    if dag_run and dag_run.conf and 'run_date' in dag_run.conf:
        target_date_str = dag_run.conf['run_date']
        target_date = datetime.strptime(target_date_str, '%Y-%m-%d').date()
    else:
        # Fallback: current time minus 1 day
        target_date = datetime.utcnow().date() - timedelta(days=1)
    
    # Formats to push
    date_formats = {
        "date_hyphen": target_date.strftime('%Y-%m-%d'),  # 2026-08-20
        "date_slash": target_date.strftime('%Y/%m/%d'),   # 2026/08/20
        "date_flat": target_date.strftime('%Y%m%d'),      # 20260820
    }
    
    return date_formats