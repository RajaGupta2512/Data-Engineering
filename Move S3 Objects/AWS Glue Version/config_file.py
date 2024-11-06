# siq_hfr_config_file.py

# Configuration dictionary for different environments
config = {
    'DEV': {
        'bucket-name': 'myawsbucket12432546',
        'source-path': 'nextgen/source/landing/',
        'target-path': 'nextgen/source/landing/archive/',
        'use_case_id': '01',
    },
    'PROD': {
        'bucket-name': 'dev-s3-bucket',
        'source-path': 'dev/source/integrated/path/',
        'target-path': 'dev/temp/integrated/path/',
        'use_case_id': '02',
    },
    'TEST': {
        'bucket-name': 'dev-s3-bucket',
        'source-path': 'dev/source/integrated/path/',
        'target-path': 'dev/temp/integrated/path/',
        'use_case_id': '03',
    }
}

# Parameters to be used by the cloud watch handler
# params = {
#     'DEV': {
#         'log_group_name': '/aws/glue/dev-logs',
#         'log_stream': 'dev-log-stream'
#     },
#     'PROD': {
#         'log_group_name': '/aws/glue/prod-logs',
#         'log_stream': 'prod-log-stream'
#     },
#     'TEST': {
#         'log_group_name': '/aws/glue/test-logs',
#         'log_stream': 'test-log-stream'
#     }
# }
