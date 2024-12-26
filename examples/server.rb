require 'sinatra'
require 'sinatra/cors'
require 'json'
require 'dotenv'
require_relative '../lib/quill'

Dotenv.load(File.expand_path('../.env', __dir__))

set :port, 3007
set :bind, '0.0.0.0'

configure do
  enable :cors
  set :allow_methods, 'GET,POST,OPTIONS'
  set :allow_origin, '*'
  set :allow_headers, 'Content-Type, Authorization'
end

quill = Quill.new(
  private_key: ENV['PRIVATE_KEY'] || '',
  database_connection_string: ENV['DB_URL'] || '',
  database_config: {},
  database_type: ENV['DB_TYPE']
)

get '/' do
  'Hello World!'
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
