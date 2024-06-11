import re


def clean_log_data(log_data):
    right_pattern = re.compile(r'^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z) (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z) (INFO) (\d+) --- \[(.*?)\] (\S+) : (.*)$')
    timestamp_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z')
    another_timestamp_pattern = re.compile(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z \}\]')
    another2_timestamp_pattern = re.compile(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z \]')

    cleaned_data = []
    stored_lines = []
    is_storing = False
    
    for line in log_data:
        if right_pattern.match(line.strip()):
            if line.endswith(']'):
                cleaned_data.append(line)
            else:
                stored_lines.append(line)
                is_storing = True

        elif 'DEBUG' in line.strip():
            continue

        elif '"Heartbeat"' in line.strip() and line.strip().endswith("{"):
            edit = line + ' }]'
            cleaned_data.append(edit)

        elif line.strip().endswith("{") or line.strip().endswith("["):
            stored_lines.append(line.strip())
            is_storing = True

        elif timestamp_pattern.match(line.strip()) and line.strip().endswith("]"):
            stored_lines.append(line.strip())
            combined_line = " ".join(stored_lines)
            cleaned_data.append(combined_line)
            stored_lines = []
            is_storing = False

        elif timestamp_pattern.match(line.strip()) and line.strip().endswith("}]"):
            stored_lines.append(line.strip())
            combined_line = " ".join(stored_lines)
            cleaned_data.append(combined_line)
            stored_lines = []
            is_storing = False
            
        elif is_storing:
            stored_lines.append(line.strip())
    
    clean_data = []
    for x in cleaned_data:

        if '[no body]'in x or '401 Unauthorized:' in x:
            continue
        elif re.match(another2_timestamp_pattern, x):
            continue
        elif re.match(another_timestamp_pattern, x):
            continue
        elif 'JDBC' in x or 'ERROR' in x:
            continue
        else:
            clean_data.append(x)
   
    return clean_data