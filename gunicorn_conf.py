# We've deliberatly limted the number of works to "1"
# Since we have a background job running that should only run once
# We need to figure out how we can extrac to a seperate worker, but that is a TODO
workers = 1
worker_connections = 1000

timeout = 0
bind = '0.0.0.0:80'
max_requests = 1000
max_requests_jitter = 50
worker_class = 'eventlet'
