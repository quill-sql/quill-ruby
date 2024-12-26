# Quill Ruby SDK

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'quill-ruby'
```

And then execute:
```bash
$ bundle install
```

## Usage Example

```ruby
require 'sinatra'
require 'sinatra/cors'
require 'json'
require 'quill'

private_key = "pk_yourprivatekeyhere12345"
database_type = "clickhouse"
database_connection_string = "https://[username]:[password]@[hostname]:[port]"

quill = Quill.new(
  private_key: private_key,
  database_type: database_type,
  database_connection_string: database_connection_string
)

configure do
  enable :cors
  set :allow_origin, '*'
  set :allow_methods, 'GET,POST,OPTIONS'
  set :allow_headers, 'Content-Type, Authorization'
end

post '/quill' do
  content_type :json
  headers 'Access-Control-Allow-Origin' => '*'
  metadata = JSON.parse(request.body.read)['metadata']
  result = quill.query(
    tenants: [metadata['orgId'] || '*'],
    metadata: metadata
  )
  result.to_json
end

options '/quill' do
  response.headers["Allow"] = "GET,POST,OPTIONS"
  response.headers["Access-Control-Allow-Origin"] = "*"
  response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
  200
end
```

## For local testing (dev purposes only)

Create a `.env` file in your project root with the following key-value pairs:

```
PRIVATE_KEY=
DB_TYPE=
DB_URL=
```

Use the following commands to start a locally hosted dev server:

```bash
bundle exec ruby server.rb
```

You should be able to ping your local server at `http://localhost:3007`.