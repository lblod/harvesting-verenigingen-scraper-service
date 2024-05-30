import multiprocessing

# Number of worker processes
workers = multiprocessing.cpu_count() * 2 + 1
worker_connections = 1000

timeout = 0
bind = '0.0.0.0:80'
workers = 4
max_requests = 1000
max_requests_jitter = 50
worker_class = 'eventlet'