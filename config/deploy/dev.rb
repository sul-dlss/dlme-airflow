# frozen_string_literal: true

# Roles are passed to docker-compose as profiles.
server 'dlme-airflow-dev.stanford.edu', user: 'dlme', roles: %w[app]

Capistrano::OneTimeKey.generate_one_time_key!