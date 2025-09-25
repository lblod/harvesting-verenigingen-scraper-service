workers = 1
worker_connections = 1000

timeout = 0
bind = '0.0.0.0:80'
max_requests = 1000
max_requests_jitter = 50
worker_class = 'eventlet'
